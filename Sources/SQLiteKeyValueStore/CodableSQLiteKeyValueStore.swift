//
//  File.swift
//  
//
//  Created by Benjamin Garrigues on 01/09/2022.
//

import Foundation
import SQLiteDatabase


open class CodableSQLiteKeyValueStore: SQLiteKeyValueStore {

    public var jsonEncoder = JSONEncoder()
    public var jsonDecoder = JSONDecoder()

    open func setCodable<V: Codable>(
        value: V?,
        forKey key: String,
        inNamespace namespace: Namespace,
        parentAccess: SQLiteQueriable? = nil
    ) throws {
        if let value = value {
            let encodedValue = try jsonEncoder.encode(value)
            try set(value: encodedValue, 
                    forKey: key,
                    inNamespace: namespace,
                    parentAccess: parentAccess)
        } else {
            try delete(valuesForKeys: [key], 
                       inNamespace: namespace,
                       parentAccess: parentAccess)
        }

    }

    open func setCodable<V: Codable>(
        valuesForKeys: [String:V],
        inNamespace namespace: Namespace
    ) throws {
        try setCodable(valuesForKeys: valuesForKeys, inNamespace: namespace, parentAccess: nil)
    }

    open func setCodable<V: Codable>(
        valuesForKeys: [String:V],
        inNamespace namespace: Namespace,
        parentAccess: SQLiteQueriable?
    ) throws {
        var encodedValues = [String: Data]()
        for (k, v) in valuesForKeys {
            encodedValues[k] = try jsonEncoder.encode(v)
        }

        try set(valuesForKeys: encodedValues,
                inNamespace: namespace,
                parentAccess: parentAccess)
    }

    open func valueCodable<V: Codable>(
        forKey key: String,
        inNamespace namespace: Namespace
    ) throws -> V? {
        return try valueCodable(forKey: key, inNamespace: namespace, parentAccess: nil)
    }
    open func valueCodable<V: Codable>(
        forKey key: String,
        inNamespace namespace: Namespace,
        parentAccess: SQLiteQueriable? = nil
    ) throws -> V? {
        guard let data = try value(forKey: key, inNamespace: namespace, parentAccess: parentAccess) else {
            return nil
        }
        return try jsonDecoder.decode(V.self, from: data)
    }

    open func valuesCodable<V: Codable>(
        forKeys keys: [String],
        inNamespace namespace: Namespace,
        parentAccess: SQLiteQueriable? = nil
    ) throws -> [String: V] {
        let dataPerKey: [String: Data] = try values(
            forKeys: keys,
            inNamespace: namespace,
            parentAccess: parentAccess
        )
        return try dataPerKey.mapValues { try jsonDecoder.decode(V.self, from: $0) }
    }


    open func valuesCodable<V: Codable>(
        forKeysStartingWith keyPrefix: String,
        inNamespace namespace: Namespace,
        parentAccess: SQLiteQueriable? = nil
    ) throws -> [String: V] {
        let dataPerKey: [String: Data] = try values(
            forKeysStartingWith: keyPrefix,
            inNamespace: namespace,
            parentAccess: parentAccess
        )
        return try dataPerKey.mapValues { try jsonDecoder.decode(V.self, from: $0) }
    }

    open func performCodableUpdateInSingleDbAccess<V: Codable>(
        forKey key: String,
        inNamespace namespace: Namespace,
        parentAccess: SQLiteQueriable? = nil,
        update: (inout V?) -> Void
    ) throws {
        try performUpdateInSingleDbAccess(
            forKey: key,
            inNamespace: namespace,
            parentAccess: parentAccess
        ) { data in
                do {
                    var decoded: V?
                    if let data = data {
                        decoded = try jsonDecoder.decode(V.self, from: data)
                    }
                    update(&decoded)
                    data = try decoded.map { try jsonEncoder.encode($0) }
                } catch {
                    logger(self, .error, "performCodableUpdateInSingleDbAccess encoding error : \(error)")
                }
            }
    }

    // convenience
    open func performCodableUpdateInSingleDbAccess<V: Codable>(
        forKey key: String,
        parentAccess: SQLiteQueriable? = nil,
        update: (inout V?) -> Void
    ) throws {
        try performCodableUpdateInSingleDbAccess(
            forKey: key,
            inNamespace: defaultNamespace,
            parentAccess: parentAccess,
            update: update
        )
    }

    open func listKeysAndCodableValues<V: Codable>(
        inNamespace namespace: Namespace,
        resultRange range: ClosedRange<Int64>
    ) throws -> [(String, V)] {
        var res = [(String, V)]()
        for (k, v) in try listKeysAndValues(inNamespace: namespace, resultRange: range) {
            res.append((k , try jsonDecoder.decode(V.self, from: v)))
        }
        return res
    }

    /// List all keys and values for a given namespace
    /// Batchsize: number of items selected per query
    open func listAllKeysAndCodableValues<V: Codable>(inNamespace namespace: String, batchSize: Int = 1000) throws -> [(String, V)] {
       return try listAllKeysAndCodableValues(
        inNamespace: namespace,
        batchSize: batchSize,
        parentAccess: nil)
    }

    open func listAllKeysAndCodableValues<V: Codable>(inNamespace namespace: String, batchSize: Int = 1000, parentAccess: SQLiteQueriable?) throws -> [(String, V)] {
        var res = [(String, V)]()
        for (k, v) in try listAllKeysAndValues(inNamespace: namespace, batchSize: batchSize, parentAccess: parentAccess) {
            res.append((k, try jsonDecoder.decode(V.self, from: v)))
        }
        return res
    }

    /// - Parameters:
    ///    - ts: TimeInterval since 1970
    open func codableValues<V: Codable>(
        updatedAfter ts: Int,
        resultRange range: ClosedRange<Int64>,
        inNamespace namespace: String
    ) throws -> [(key: String, value: V, updatedAt: Int)] {
        return try values(
            updatedAfter: ts,
            resultRange: range,
            inNamespace: namespace
        ).map { key, value, updatedAt in
            return (key, try jsonDecoder.decode(V.self, from: value), updatedAt)
        }
    }
}

extension CodableSQLiteKeyValueStore {
    public func setCodable<V: Codable>(
        value: V?,
        forKey key: String,
        inNamespace namespace: String
    ) throws -> Void {
        try setCodable(value: value, forKey: key, inNamespace: namespace, parentAccess: nil)
    }
    public func setCodable<V: Codable>(
        value: V?,
        forKey key: String
    ) throws -> Void {
        try setCodable(value: value, forKey: key, inNamespace: defaultNamespace, parentAccess: nil)
    }
}
