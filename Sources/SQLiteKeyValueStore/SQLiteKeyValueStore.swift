import Foundation
import SQLiteDatabase

open class SQLiteKeyValueStore {
    public typealias Namespace = String

    var dbFilename: String

    var logger: Logger
    public private(set) var dbAccess: SQLiteDatabase.Access.Blocking
    public private(set) var dbPath: URL
    var dispatchQueue: DispatchQueue
    var statements: [Namespace: NamespaceStatementSet]

    struct NamespaceStatementSet {
        // :key , :value
        let insertOrReplace: OpaquePointer
        // :key
        let delete: OpaquePointer
        // :key
        let deleteWhereKeysLikeStatement: OpaquePointer
        // :key
        let select: OpaquePointer
        // :key
        let selectWhereKeysLike: OpaquePointer

        var allStatements: [OpaquePointer] {
            return [insertOrReplace, delete, select, selectWhereKeysLike, deleteWhereKeysLikeStatement]
        }
    }

    var storeName: String

    // allowDots = false for legacy behavior.
    public class func dbPath(rootPath: URL, storeName: String, allowDots: Bool) -> (dbFileName: String, fullPath:URL) {
        let dbFileName = storeName.asAcceptableFileName(allowDots: allowDots)
        let fullPath = rootPath.appendingPathComponent(dbFileName)
        return (dbFileName, fullPath)
    }

    public init(logger: @escaping Logger,
                rootPath: URL,
                storeName: String,
                keepDotsInStoreName: Bool = true, // set to false to legacy behavior.
                accessQueueQoS queueQoS: DispatchQoS,
                journalMode: SQLiteDatabase.JournalMode? = nil,
                busyTimeoutMS: Int? = nil /// time to wait before timing out upon concurrent access lock (milliseconds)
    ) throws {
        self.logger = logger
        self.storeName = storeName
        let (dbFileName, dbPath) = Self.dbPath(rootPath: rootPath, storeName: storeName, allowDots: keepDotsInStoreName)
        self.dbFilename = dbFileName
        self.dbPath = dbPath
        dispatchQueue = DispatchQueue(label: storeName,
                                      qos: queueQoS)
        let db = SQLiteDatabase(dbPath: dbPath)
        dbAccess = try db.openBlockingAccess(queue: dispatchQueue, journalMode: journalMode, busyTimeoutMS: busyTimeoutMS)
        statements = [:]
        try prepareStatementsForExistingNamespaces()
    }
    
    /// Per https://www.sqlite.org/c3ref/close.html
    /// unfinalized prepared statements will keep the sqlite connection open.
    deinit {
        do {
            try dropAllPreparedStatements()
        } catch {
            logger(self, .error, "\(dbFilename) : could not dropAllPreparedStatements at deinit : \(error)")
        }
    }


    public var defaultNamespace: String = "default"
    private func generateStatements(
        for namespace: Namespace,
        parentAccess: SQLiteQueriable?
    ) throws -> NamespaceStatementSet {
        return NamespaceStatementSet(
            insertOrReplace: try prepareStatement(
                Self.insertOrReplaceStatement(ns: namespace),
                using: dbAccess,
                parentAccess: parentAccess
            ),
            delete: try prepareStatement(
                Self.deleteStatement(ns: namespace),
                using: dbAccess,
                parentAccess: parentAccess
            ),
            deleteWhereKeysLikeStatement: try prepareStatement(
                Self.deleteWhereKeysLikeStatement(ns: namespace),
                using: dbAccess,
                parentAccess: parentAccess
            ),
            select: try prepareStatement(
                Self.selectStatement(ns: namespace),
                using: dbAccess,
                parentAccess: parentAccess
            ),
            selectWhereKeysLike: try prepareStatement(
                Self.selectWhereKeysLikeStatement(ns: namespace),
                using: dbAccess,
                parentAccess: parentAccess
            )
        )
    }

    private func prepareStatement(
        _ sql: String,
        using dbAccess: SQLiteDatabase.Access.Blocking,
        parentAccess: SQLiteQueriable?
    ) throws -> OpaquePointer {
        return try dbAccess(parentAccess: parentAccess) {
            logger(self, .trace, "prepare statement \n\(sql)")
            return try $0.prepare(sql)
        }
    }

    private func prepareStatementsForExistingNamespaces() throws {
        try dbAccess { db in
            for namespace in try listNamespaces(parentAccess: db) {
                statements[namespace] = try generateStatements(for: namespace,
                                                               parentAccess: db)
            }
        }
    }

    private func finalizePreparedStatementsForNamespace(_ ns: Namespace, parentAccess: SQLiteQueriable? = nil) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            if let statement = statements[ns] {
                try statement.allStatements.forEach { try db.finalize($0) }
                statements.removeValue(forKey: ns)
            }
        }
    }

    private func dropAllPreparedStatements() throws {
        try dbAccess { db in
            for ns in try listNamespaces(parentAccess: db) {
                try finalizePreparedStatementsForNamespace(ns, parentAccess: db)
            }
        }
    }

    func listNamespaces(parentAccess: SQLiteQueriable? = nil) throws -> [Namespace] {
        var res = [Namespace]()
        try dbAccess(parentAccess: parentAccess) { db in
            try db.exec(Self.listNamespacesStatement(),
                    with: [:]) { statement in
                let dataCol = try db.get(0, in: statement)
                guard
                    let namespace: String = dataCol.value() else {
                    self.logger(self, .error, "Could not fetch result \(dataCol) is not string")
                        return
                }
                res.append(namespace)
            }
        }
        return res
    }

    func countKeys(inNamespace ns: Namespace, parentAccess: SQLiteQueriable? = nil) throws -> Int {
        return try dbAccess(parentAccess: parentAccess) { db in
            _ = try prepareStatementsIfNeeded(for: ns, parentAccess: db)
            return try db.selectSingle(Self.countAllStatement(ns: ns), withInput: [:]).value() ?? 0
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
        inNamespace namespace: String,
        parentAccess: SQLiteQueriable? = nil,
        update: (inout Data?) -> Void
    ) throws {
        return try performInSingleDbAccess(parentAccess: parentAccess) { db in
            var val = try value(forKey: key, inNamespace: namespace, parentAccess: db)
            update(&val)
            if let val {
                try set(value: val, forKey: key, inNamespace: namespace, parentAccess: db)
            } else {
                try delete(valuesForKeys: [key], inNamespace: namespace, parentAccess: db)
            }
        }
    }

    /// Backups database serially with read/write operations
    /// We are not using official SQLite's backuping methods
    /// (see: `https://www.sqlite.org/c3ref/backup_finish.html#sqlite3backupinit`)
    /// as this db is not accessed by any other process nor thread, thus copying the db file
    /// synchronously with other operations ensures we are not within a db transaction.
    public func backupAssumingExclusiveAccess(to url: URL, overwriteIfExists: Bool = true) throws {
        try dispatchQueue.sync {
            if overwriteIfExists,
               FileManager.default.fileExists(atPath: url.path) {

                guard let tmpPath = (FileManager.default.temporaryDirectory as NSURL).appendingPathComponent(UUID().uuidString) else {
                    logger(self, .error, "could not create temporary file to backup \(self.dbPath) to")
                    return
                }
                defer {
                    try? FileManager.default.removeItem(at: tmpPath)
                }
                // Copy our file to a tmp file
                try FileManager.default.copyItem(at: dbPath, to: tmpPath)
                // Atomically replace existing file with our tmp file (which moves tmp files to new location)
                _ = try FileManager.default.replaceItemAt(url, withItemAt: tmpPath)
            } else {
                try FileManager.default.copyItem(at: dbPath, to: url)
            }
        }
    }
}

extension SQLiteKeyValueStore {

    enum StoreError: Error {
        case nilPreparedStatements(namespace: Namespace)
    }

    func prepareStatementsIfNeeded(for namespace: Namespace,
                                   parentAccess: SQLiteQueriable?) throws -> NamespaceStatementSet  {
        if let stmts = statements[namespace] { return stmts }

        // create the table for the namespace
        try dbAccess(parentAccess: parentAccess) { db in
            try db.exec(Self.createNamespaceTableStatement(ns: namespace))
        }

        let res = try generateStatements(for: namespace, parentAccess: parentAccess)
        statements[namespace] = res
        return res
    }

    public func set(value: Data, forKey key: String, inNamespace namespace: String) throws {
        try set(value: value, forKey: key, inNamespace: namespace, parentAccess: nil)
    }

    public func set(value: Data, forKey key: String, inNamespace namespace: String, parentAccess: SQLiteQueriable?) throws {
        try dbAccess(parentAccess: parentAccess){ db in
            let statements = try prepareStatementsIfNeeded(for: namespace, parentAccess: db)
            try db.exec(statements.insertOrReplace, with: [":key" : .init(key), ":value": .init(value)])
        }
    }
    public func set(valuesForKeys: [String : Data],
                    inNamespace namespace: String) throws {
        try set(valuesForKeys: valuesForKeys, inNamespace: namespace, parentAccess: nil)
    }

    public func set(valuesForKeys: [String : Data],
                    inNamespace namespace: String,
                    parentAccess: SQLiteQueriable?) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            let statements = try prepareStatementsIfNeeded(for: namespace, parentAccess: db)
            try db.bulkExec(statements.insertOrReplace,
                            withInput: valuesForKeys.map{
                                [":key" : .init($0), ":value": .init($1)]
            })
        }
    }

    public func value(forKey key: String, inNamespace namespace: String) throws -> Data? {
        return try value(forKey: key, inNamespace: namespace, parentAccess: nil)
    }
    public func values(forKeys keys: [String], inNamespace namespace: String, parentAccess: SQLiteQueriable?) throws -> [String: Data] {
        var res = [String: Data]()
        try dbAccess(parentAccess: parentAccess) { db in
            res = Dictionary(
                uniqueKeysWithValues: try keys.compactMap {
                    guard let value = try value(forKey: $0, inNamespace: namespace, parentAccess: db) else { return nil }
                    return ($0, value)
                }
            )
        }
        return res
    }

    public func value(forKey key: String, inNamespace namespace: String, parentAccess: SQLiteQueriable?) throws -> Data? {
        var res: Data?
        try dbAccess(parentAccess: parentAccess) { db in
            let statements = try prepareStatementsIfNeeded(for: namespace, parentAccess: db)
            try db.exec(
                statements.select,
                with: [":key": .init(key)]
            ) { (stmt) in
                let dataCol = try db.get(1, in: stmt)
                guard
                    let data: Data = dataCol.value() else {
                    self.logger(self, .error, "Could not fetch value for key \(key) \(dataCol) is not of type Data")
                    return
                }
                res = data
            }
        }
        return res
    }

    /// - Parameters:
    ///    - ts: TimeInterval since 1970
    public func values(
        updatedAfter ts: Int,
        resultRange range: ClosedRange<Int64>,
        inNamespace namespace: String
    ) throws -> [(key: String, value: Data, updatedAt: Int)] {
        var res = [(key: String, value: Data, updatedAt: Int)]()
        try dbAccess { (db) in
            try db.exec(Self.selectUpdatedAfterSQL(ns: namespace),
                        with: [ ":updated_at": .int64(Int64(ts)),
                                ":offset": .int64(range.lowerBound),
                                ":count": .int64(Int64(range.count))]
            ) { (stmt) in
                guard let key: String = try db.get(0, in : stmt).value(),
                      let value: Data = try db.get(1, in : stmt).value(),
                      let updatedAt: Int = try db.get(2, in : stmt).value() else {
                    self.logger(self, .error, "nil key/value/updatedat on select in range result")
                    return
                }
                res.append((key, value, updatedAt))
            }
        }
        return res
    }

    public func values(
        forKeysStartingWith keyPrefix: String,
        inNamespace namespace: String,
        parentAccess: SQLiteQueriable?
    ) throws -> [String: Data] {
        var res = [String: Data]()
        try dbAccess(parentAccess: parentAccess) { db in
            let statements = try prepareStatementsIfNeeded(for: namespace, parentAccess: db)
            try db.exec(
                statements.selectWhereKeysLike,
                with: [":key": .init("\(keyPrefix)%")]
            ) { (stmt) in
                let keyCol = try db.get(0, in: stmt)
                let dataCol = try db.get(1, in: stmt)
                guard
                    let key: String = keyCol.value(),
                    let data: Data = dataCol.value() else {
                    self.logger(
                        self, .error, "Could not fetch string value for key \(keyCol)  or \(dataCol) is not of type Data"
                    )
                    return
                }
                res[key] = data
            }
        }
        return res
    }

    public func delete(valuesForKeys keys: [String], inNamespace namespace: String) throws {
        try delete(valuesForKeys: keys, inNamespace: namespace, parentAccess: nil)
    }

    public func delete(valuesForKeys keys: [String], inNamespace namespace: String, parentAccess: SQLiteQueriable?) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            let statements = try prepareStatementsIfNeeded(for: namespace, parentAccess: db)
            try db.bulkExec(statements.delete,
                            withInput: keys.map { [":key": .init($0)] })
        }
    }

    public func delete(valuesForKeysStartingWith keyPrefix: String, inNamespace namespace: String, parentAccess: SQLiteQueriable?) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            let statements = try prepareStatementsIfNeeded(for: namespace, parentAccess: db)
            try db.bulkExec(statements.deleteWhereKeysLikeStatement, withInput: [[":key": .init("\(keyPrefix)%")]])
        }
    }

    /// This version of the method is needed to comply with the protocol
    public func delete(allValuesInNamespace namespace: String) throws {
        try delete(allValuesInNamespace: namespace, parentAccess: nil)
    }

    public func delete(allValuesInNamespace namespace: String, parentAccess: SQLiteQueriable?) throws {
        try dbAccess(parentAccess: parentAccess){ db in
            guard try listNamespaces(parentAccess: db).contains(namespace) else { return }
            try db.exec(Self.deleteAllStatement(ns: namespace), with: [:])
        }
    }

    public func delete(namespace: String) throws {
        try delete(namespace: namespace, parentAccess: nil)
    }

    public func delete(namespace: String, parentAccess: SQLiteQueriable? = nil) throws {
        try dbAccess(parentAccess: parentAccess) { db in
            try db.exec(Self.dropStatement(ns: namespace), with: [:])
            try finalizePreparedStatementsForNamespace(namespace, parentAccess: db)
        }
    }

    public func deleteAllNamespaces() throws {
        try dbAccess { db  in
            for ns in try listNamespaces(parentAccess: db) {
                try delete(namespace: ns, parentAccess: db)
            }
        }
    }

    public func listKeysAndValues(inNamespace namespace: String, resultRange range: ClosedRange<Int64>) throws -> [(String, Data)] {
        return try listKeysAndValues(inNamespace: namespace, resultRange: range, parentAccess: nil)
    }

    public func listKeysAndValues(inNamespace namespace: String, resultRange range: ClosedRange<Int64>, parentAccess: SQLiteQueriable? = nil) throws -> [(String, Data)] {
        var res = [(String, Data)]()
        try dbAccess(parentAccess: parentAccess){ db in
            _ = try prepareStatementsIfNeeded(for: namespace, parentAccess: db)

            try db.exec(Self.selectAllStatement(ns: namespace), with: [":offset": .int64(range.lowerBound), ":count": .int64(Int64(range.count))]) { statement in

                let keyCol = try db.get(0, in: statement)
                guard
                    let key: String = keyCol.value() else {
                    self.logger(
                        self, .error,
                        "listKeysAndValues : could not fetch result \(keyCol) is not of type String"
                    )
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
    public func listAllKeysAndValues(inNamespace namespace: String, batchSize: Int) throws -> [(String, Data)] {
        return try listAllKeysAndValues(
            inNamespace: namespace,
            batchSize: batchSize,
            parentAccess: nil
        )
    }
    
    public func listAllKeysAndValues(inNamespace namespace: String, batchSize: Int, parentAccess: SQLiteQueriable?) throws -> [(String, Data)] {
        let batchSize = Int64(batchSize)
        var res = [(String, Data)]()
        try dbAccess(parentAccess: parentAccess) { db in

            var range = 0...(batchSize - 1)
            var reads = [(String, Data)]()
            repeat {
                reads = try listKeysAndValues(
                    inNamespace: namespace,
                    resultRange: range,
                    parentAccess: db
                )
                res.append(contentsOf: reads)
                range = (range.lowerBound + batchSize)...(range.upperBound + batchSize)
            } while reads.count > 0
        }
        return res
    }

    public func numberOfKeys(inNamespace namespace: String) throws -> Int {
        return try countKeys(inNamespace: namespace)
    }

    public func listNamespaces() throws -> [String] {
        return try listNamespaces(parentAccess: nil)
    }

    public func numberOfKeysPerNamespace() throws -> [String: Int] {
        var res = [String: Int]()
        try dbAccess { db in
            try listNamespaces(parentAccess: db).forEach({ ns in
                res[ns] = try self.countKeys(inNamespace: ns, parentAccess: db)
            })
        }
        return res
    }

    public func createUpdatedAtIndexesInNotExistsForAllTables() throws {
        try dbAccess { db in
            var tables = [String]()
            try db.exec("""
                SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '\(SQLiteKeyValueStore.namespaceTablePrefix)%'
            """, with: [:]) { statement in
                guard let namespaceTable: String = try db.get(0, in: statement).value() else { return }
                tables.append(namespaceTable)
            }

            try tables.forEach {
                try db.exec("""
                CREATE INDEX IF NOT EXISTS "idx_\($0)_updated_at" ON "\($0)"(updated_at);
                """)
            }
        }
    }
}
