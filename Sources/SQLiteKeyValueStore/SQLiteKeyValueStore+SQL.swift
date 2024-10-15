//
//  File.swift
//  
//
//  Created by Benjamin Garrigues on 01/09/2022.
//

import Foundation
extension SQLiteKeyValueStore {

    static var namespaceTablePrefix: String = "ns_"
    static func table(forNamespace ns: Namespace) -> String { return "\(namespaceTablePrefix)\(ns)" }

    static func createNamespaceTableStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
        CREATE TABLE IF NOT EXISTS "\(table)" (
           key TEXT PRIMARY KEY NOT NULL,
           value BLOB NOT NULL,
           updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        );

        CREATE INDEX IF NOT EXISTS "idx_\(table)_updated_at" ON "\(table)"(updated_at);
        """
    }

    static func insertOrReplaceStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            INSERT OR REPLACE INTO "\(table)" (key, value)
            VALUES ( :key, :value ) ;
        """
    }

    static func deleteStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            DELETE FROM "\(table)"
            WHERE KEY = :key ;
        """
    }
    static func deleteWhereKeysLikeStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            DELETE FROM "\(table)"
            WHERE KEY LIKE :key ;
        """
    }

    static func deleteAllStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            DELETE FROM "\(table)";
        """
    }

    static func dropStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            DROP TABLE IF EXISTS "\(table)";
        """
    }

    static func selectStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            SELECT key, value FROM "\(table)"
            WHERE key = :key ;
        """
    }

    static func selectUpdatedAfterSQL(ns: Namespace) -> String {
        return """
            SELECT key, value, updated_at
            FROM \(table(forNamespace: ns))
            WHERE updated_at >= :updated_at
            ORDER BY updated_at LIMIT :count OFFSET :offset ;
        """
    }

    static func selectWhereKeysLikeStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            SELECT key, value FROM "\(table)"
            WHERE key LIKE :key ;
        """
    }

    static func selectAllStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            SELECT key, value FROM "\(table)" ORDER BY ROWID LIMIT :count OFFSET :offset;
        """
    }

    static func countAllStatement(ns: Namespace) -> String {
        let table = table(forNamespace: ns)
        return """
            SELECT count(*) FROM "\(table)";
        """
    }

    static func listNamespacesStatement() -> String {
        return """
            SELECT substr(name, \(namespaceTablePrefix.count + 1)) FROM sqlite_master WHERE type='table' AND name LIKE '\(namespaceTablePrefix)%' ORDER BY name;
        """
    }
}
