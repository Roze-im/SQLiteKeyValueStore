//
//  File.swift
//  
//
//  Created by Benjamin Garrigues on 28/07/2023.
//

import Foundation
extension ExpiringSQLiteKeyValueStore {


    static func createTableIfNotExistsSQL() -> String {
        return """
                CREATE TABLE IF NOT EXISTS store (
                   key TEXT PRIMARY KEY NOT NULL,
                   value BLOB NOT NULL,
                   expires_at INTEGER NOT NULL,
                   updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
                );
                CREATE INDEX IF NOT EXISTS "idx_store_expires_at" ON store(expires_at);
                CREATE INDEX IF NOT EXISTS "idx_store_updated_at" ON store(updated_at);
            """
    }

    static func insertOrReplaceSQL() -> String {
        return """
            INSERT OR REPLACE INTO store (key, value, expires_at)
            VALUES ( :key, :value, :expires_at ) ;
        """
    }

    static func deleteSQL() -> String {
        return """
            DELETE FROM store
            WHERE KEY = :key;
        """
    }
    static func deleteWhereKeysLikeSQL() -> String {
        return """
            DELETE FROM store
            WHERE KEY LIKE :key ;
        """
    }

    static func deleteExpiredSQL() -> String {
        if #available(iOS 16, *) {
            return """
            DELETE FROM store
            WHERE expires_at <= UNIXEPOCH();
            """
        } else {
            return """
            DELETE FROM store
            WHERE expires_at <= strftime('%s', 'now');
            """
        }
    }

    static func deleteAllSQL() -> String {
        return """
            DELETE FROM store;
        """
    }

    static func selectSQL() -> String {
        if #available(iOS 16, *) {
            return """
                SELECT key, value FROM store
                WHERE key = :key
                AND expires_at > UNIXEPOCH();
            """
        } else {
            return """
                SELECT key, value FROM store
                WHERE key = :key
                AND expires_at > strftime('%s', 'now');
            """
        }
    }

    static func selectWhereKeysLikeSQL() -> String {
        if #available(iOS 16, *) {
            return """
                SELECT key, value FROM store
                WHERE key LIKE :key
                AND expires_at > UNIXEPOCH();
            """
        } else {
            return """
                SELECT key, value FROM store
                WHERE key LIKE :key
                AND expires_at > strftime('%s', 'now');
            """
        }
    }

    // "now" needs to be provided because you may want
    // to keep it stable across pages of results.
    static func selectAllSQL() -> String {
        return """
            SELECT key, value
            FROM store
            WHERE expires_at > :now
            ORDER BY KEY LIMIT :count OFFSET :offset;
        """
    }

    static func countAllSQL() -> String {
        if #available(iOS 16, *) {
            return """
                SELECT count(*)
                FROM store
                WHERE expires_at > UNIXEPOCH();
            """
        } else {
            return """
                SELECT count(*)
                FROM store
                WHERE expires_at > strftime('%s', 'now');
            """
        }
    }

}
