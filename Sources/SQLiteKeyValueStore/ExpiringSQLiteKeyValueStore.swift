//
//  ExpiringSQLiteKeyValueStore.swift
//  
//
//  Created by Benjamin Garrigues on 28/07/2023.
//

import Foundation
import SQLiteDatabase


/// An expiring keyvalue store is a simplified key value store
/// to be used as a cache.
/// It doesn't support namespaces, but support "expires_at" parameters on keys.
/// It automatically prunes expired values at init and filters them out at
/// selection otherwise
/// One can manually call "deleteExpiredEntries" to perform the garbage collection.
public class ExpiringSQLiteKeyValueStore {
    var dbFilename: String

    var logger: Logger
    var dbAccess: SQLiteDatabase.Access.Blocking
    var dbPath: URL
    public internal(set) var dispatchQueue: DispatchQueue
    var preparedStatements: StatementSet

    struct StatementSet {
        // :key , :value
        let insertOrReplace: OpaquePointer
        // :key
        let delete: OpaquePointer
        // :key
        let deleteWhereKeysLike: OpaquePointer
        // :key
        let select: OpaquePointer
        // :key
        let selectWhereKeysLike: OpaquePointer
        // :count, :offset
        let selectAll: OpaquePointer

        var allStatements: [OpaquePointer] {
            return [insertOrReplace, delete, select, selectWhereKeysLike, selectAll, deleteWhereKeysLike]
        }
    }

    var storeName: String
    let defaultExpiringTime: TimeInterval
    let codableEncoder = JSONEncoder()
    let codableDecoder = JSONDecoder()

    public init(logger: @escaping Logger,
                rootPath: URL,
                storeName: String,
                defaultExpiringTime: TimeInterval,
                accessQueueQoS queueQoS: DispatchQoS,
                journalMode: SQLiteDatabase.JournalMode? = nil,
                busyTimeoutMS: Int? = nil /// time to wait before timing out upon concurrent access lock (milliseconds)
    ) throws {
        self.logger = logger
        self.storeName = storeName
        self.defaultExpiringTime = defaultExpiringTime
        dbFilename = storeName.asAcceptableFileName()
        dbPath = rootPath.appendingPathComponent(dbFilename)
        dispatchQueue = DispatchQueue(label: storeName,
                                      qos: queueQoS)
        let db = SQLiteDatabase(dbPath: dbPath)
        dbAccess = try db.openBlockingAccess(queue: dispatchQueue, journalMode: journalMode, busyTimeoutMS: busyTimeoutMS)
        preparedStatements = try dbAccess {
            try $0.exec(Self.createTableIfNotExistsSQL())

            let preparedStatements = try Self.prepareAllStatements(
                db: $0, logger: logger
            )
            try Self.performDeleteExpiredValues(in: $0)
            return preparedStatements
        }
    }

    /// Per https://www.sqlite.org/c3ref/close.html
    /// unfinalized prepared statements will keep the sqlite connection open.
    deinit {
        do {
            try finalizePreparedStatements()
        } catch {
            logger(self, .error, "could not finalizePreparedStatements at deinit : \(error)")
        }
    }

    private static func prepareAllStatements(
        db: SQLiteQueriable,
        logger: Logger
    ) throws -> StatementSet {
        return StatementSet(
            insertOrReplace: try prepareStatement(
                Self.insertOrReplaceSQL(),
                db: db,
                logger: logger
            ),
            delete: try prepareStatement(
                Self.deleteSQL(),
                db: db,
                logger: logger
            ),
            deleteWhereKeysLike: try prepareStatement(
                Self.deleteWhereKeysLikeSQL(),
                db: db,
                logger: logger
            ),
            select: try prepareStatement(
                Self.selectSQL(),
                db: db,
                logger: logger
            ),
            selectWhereKeysLike: try prepareStatement(
                Self.selectWhereKeysLikeSQL(),
                db: db,
                logger: logger
            ),
            selectAll: try prepareStatement(
                Self.selectAllSQL(),
                db: db,
                logger: logger
            )

        )
    }

    private static func prepareStatement(
        _ sql: String,
        db: SQLiteQueriable,
        logger: Logger
    ) throws -> OpaquePointer {
        logger(self, .trace, "prepare statement \n\(sql)")
        return try db.prepare(sql)
    }


    private func finalizePreparedStatements(parentAccess: SQLiteQueriable? = nil) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            try preparedStatements.allStatements.forEach { try db.finalize($0) }
        }
    }
    func countKeys(parentAccess: SQLiteQueriable? = nil) throws -> Int {
        return try dbAccess(parentAccess: parentAccess) { db in
            return try db.selectSingle(Self.countAllSQL(), withInput: [:]).value() ?? 0
        }
    }

    /// Schedule a set of operations to be performed in the same access to the db
    /// WARNING : the SQLiteQueriable objet has to be manually provided to all the calls within the closure.
    public func performInSingleDbAccess(
        parentAccess: SQLiteQueriable? = nil,
        closure: (SQLiteQueriable) throws -> Void
    ) throws {
        return try dbAccess(parentAccess: parentAccess) { db in
            try closure(db)
        }
    }

    public func performUpdateInSingleDbAccess(
        forKey key: String,
        parentAccess: SQLiteQueriable? = nil,
        update: (inout Data?) -> Void
    ) throws {
        return try performInSingleDbAccess(parentAccess: parentAccess) { access in
            var val = try value(forKey: key, parentAccess: access)
            update(&val)
            if let val {
                try set(value: val, forKey: key, parentAccess: access)
            } else {
                try delete(valuesForKeys: [key], parentAccess: access)
            }
        }
    }

    public func performCodableUpdateInSingleDbAccess<V: Codable>(
        forKey key: String,
        parentAccess: SQLiteQueriable? = nil,
        update: (inout V?) -> Void
    ) throws {
        try performUpdateInSingleDbAccess(
            forKey: key,
            parentAccess: parentAccess
        ) { data in
                do {
                    var decoded: V?
                    if let data = data {
                        decoded = try codableDecoder.decode(V.self, from: data)
                    }
                    update(&decoded)
                    data = try decoded.map { try codableEncoder.encode($0) }
                } catch {
                    logger(self, .error, "performCodableUpdateInSingleDbAccess encoding error : \(error)")
                }
            }
    }

    // MARK: Setters
    public func set(
        value: Data,
        forKey key: String,
        expiresIn: TimeInterval? = nil,
        parentAccess: SQLiteQueriable? = nil
    ) throws {
        try dbAccess(parentAccess: parentAccess){ db in
            let expiresAt = Date().timeIntervalSince1970 + (expiresIn ?? defaultExpiringTime)
            try db.exec(preparedStatements.insertOrReplace,
                        with: [
                            ":key" : .init(key),
                            ":value": .init(value),
                            ":expires_at": .int64(Int64(expiresAt))])
        }
    }
    
    public func set(value: Data?, forKey key: String) throws {
        if let value = value {
            try set(value: value, forKey: key)
        } else {
            try delete(valuesForKeys: [key])
        }
    }

    public func set(valuesForKeys: [String : Data],
                    expiresIn: TimeInterval? = nil,
                    parentAccess: SQLiteQueriable? = nil) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            let expiresAt = Int64(Date().timeIntervalSince1970 + (expiresIn ?? defaultExpiringTime))
            try db.bulkExec(preparedStatements.insertOrReplace,
                            withInput: valuesForKeys.map{
                [":key" : .init($0),
                 ":value": .init($1),
                 ":expires_at": .int64(expiresAt)
                ]
            })
        }
    }

    // MARK: Getters
    public func values(forKeys keys: [String], parentAccess: SQLiteQueriable? = nil) throws -> [String: Data] {
        var res = [String: Data]()
        try dbAccess(parentAccess: parentAccess) { db in
            res = Dictionary(
                uniqueKeysWithValues: try keys.compactMap {
                    guard let value = try value(forKey: $0, parentAccess: db) else { return nil }
                    return ($0, value)
                }
            )
        }
        return res
    }

    public func value(forKey key: String, parentAccess: SQLiteQueriable? = nil) throws -> Data? {
        var res: Data?
        try dbAccess(parentAccess: parentAccess) { db in
            try db.exec(
                preparedStatements.select,
                with: [":key": .init(key)]
            ) { (stmt) in
                let dataCol = try db.get(1, in: stmt)
                guard
                    let data: Data = dataCol.value() else {
                    self.logger(self, .error,
                                      "Could not fetch value for key \(key) \n" +
                                      "\(dataCol) is not of type Data")
                    return
                }
                res = data
            }
        }
        return res
    }

    public func values(
        forKeysStartingWith keyPrefix: String,
        parentAccess: SQLiteQueriable? = nil
    ) throws -> [String: Data] {
        var res = [String: Data]()
        try dbAccess(parentAccess: parentAccess) { db in
            try db.exec(
                preparedStatements.selectWhereKeysLike,
                with: [":key": .init("\(keyPrefix)%")]
            ) { (stmt) in
                let keyCol = try db.get(0, in: stmt)
                let dataCol = try db.get(1, in: stmt)
                guard
                    let key: String = keyCol.value(),
                    let data: Data = dataCol.value() else {
                    self.logger(
                        self, .error,
                        "Could not fetch string value for key \(keyCol) \n" + " or \(dataCol) is not of type Data"
                    )
                    return
                }
                res[key] = data
            }
        }
        return res
    }

    // MARK: Codable set / get
    public func setCodable<T: Encodable>(
        value: T,
        forKey key: String,
        expiresIn: TimeInterval? = nil,
        parentAccess: SQLiteQueriable? = nil
    ) throws {
        try set(
            value: codableEncoder.encode(value),
            forKey: key,
            expiresIn: expiresIn,
            parentAccess: parentAccess
        )
    }

    public func setCodable<T: Encodable>(
        valuesForKeys: [String : T],
        expiresIn: TimeInterval? = nil,
        parentAccess: SQLiteQueriable? = nil
    ) throws {
        try set(
            valuesForKeys: valuesForKeys.mapValues {
                try codableEncoder.encode($0)
            },
            expiresIn: expiresIn,
            parentAccess: parentAccess
        )
    }

    public func codableValue<T: Decodable>(forKey key: String, parentAccess: SQLiteQueriable? = nil) throws -> T? {
        guard let res = try value(forKey: key, parentAccess: parentAccess) else { return nil }
        return try codableDecoder.decode(T.self, from: res)
    }

    public func codableValues<T: Decodable>(forKeys keys: [String], parentAccess: SQLiteQueriable? = nil) throws -> [String: T] {
        return try values(forKeys: keys, parentAccess: parentAccess).mapValues {
            try codableDecoder.decode(T.self, from: $0)
        }
    }

    public func codableValues<T: Decodable>(
        forKeysStartingWith keyPrefix: String,
        parentAccess: SQLiteQueriable? = nil
    ) throws -> [String: T] {
        return try values(forKeysStartingWith: keyPrefix, parentAccess: parentAccess).mapValues {
            try codableDecoder.decode(T.self, from: $0)
        }
    }


    // MARK: Delete
    public func delete(valuesForKeys keys: [String]) throws {
        try delete(valuesForKeys: keys, parentAccess: nil)
    }

    public func delete(valuesForKeys keys: [String], parentAccess: SQLiteQueriable? = nil) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            try db.bulkExec(preparedStatements.delete,
                            withInput: keys.map { [":key": .init($0)] })
        }
    }

    public func delete(valuesForKeysStartingWith keyPrefix: String, parentAccess: SQLiteQueriable? = nil) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            try db.bulkExec(preparedStatements.deleteWhereKeysLike, withInput: [[":key": .init("\(keyPrefix)%")]])
        }
    }

    public func deleteExpiredValues(parentAccess: SQLiteQueriable?) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            try Self.performDeleteExpiredValues(in: db)
        }
    }

    public func deleteAllValues(parentAccess: SQLiteQueriable? = nil) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            try db.exec(Self.deleteAllSQL())
        }
    }

    static func performDeleteExpiredValues(in db: SQLiteQueriable) throws {
        try db.exec(deleteExpiredSQL(), with: [:])
    }

    public func listKeysAndValues(now: Date = Date(), resultRange range: ClosedRange<Int64>, parentAccess: SQLiteQueriable? = nil) throws -> [(String, Data)] {
            var res = [(String, Data)]()
            try dbAccess(parentAccess: parentAccess){ db in
                try db.exec(preparedStatements.selectAll,
                            with: [":offset": .int64(range.lowerBound), ":count": .int64(Int64(range.count))]) { statement in

                    let keyCol = try db.get(0, in: statement)
                    guard
                        let key: String = keyCol.value() else {
                        self.logger(self, .error,
                                          "listKeysAndValues : could not fetch result \n" +
                                          "\(keyCol) is not of type String")
                        return
                    }
                    let valueCol = try db.get(1, in: statement)
                    guard let value: Data = valueCol.value() else {
                        self.logger(self, .error, "listKeysAndValues : could not fetch result \n" +
                                                    "\(valueCol) is not of type Data")
                        return
                    }
                    res.append((key, value))
                }
            }
            return res
        }

    /// List all keys and values for a given namespace
    /// Batchsize: number of items selected per query
    public func listAllKeysAndValues(batchSize: Int) throws -> [(String, Data)] {
        let batchSize = Int64(batchSize)
        var res = [(String, Data)]()
        try dbAccess { db in

            var range = 0...(batchSize - 1)
            var reads = [(String, Data)]()
            repeat {
                reads = try listKeysAndValues(
                    resultRange: range,
                    parentAccess: db
                )
                res.append(contentsOf: reads)
                range = (range.lowerBound + batchSize)...(range.upperBound + batchSize)
            } while reads.count > 0
        }
        return res
    }

    public func numberOfKeys() throws -> Int {
        return try countKeys()
    }

    // MARK: Misc
    public func mockNow(as date: Date) throws {
        logger(self, .warning, "MOCKING UNIXEPOCH TO \(date)")
        try dbAccess {
            try $0.mockUnixEpochFunction(with: Int32(date.timeIntervalSince1970))
        }
    }
}

extension String {
    func asAcceptableFileName(allowDots: Bool = false) -> String {
        let passed = self.unicodeScalars.filter {
            (allowDots && ($0 == ".")) || CharacterSet.alphanumerics.contains($0)
        }
        return String(passed)
    }

}
