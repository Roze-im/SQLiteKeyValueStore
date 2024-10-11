import XCTest
@testable import SQLiteKeyValueStore

final class ExpiringSQLiteKeyValueStoreTests: XCTestCase {
    func makeStore(name: String = "testStore", expiringTime: TimeInterval) -> ExpiringSQLiteKeyValueStore {
        return try! .init(
            logger: { print("[\($1)] \(Date()) \(String(describing: $0)) \($2)") },
            rootPath: FileManager.default.userDocumentsDirPath,
            storeName: name,
            defaultExpiringTime: expiringTime,
            accessQueueQoS: .userInitiated
        )
    }

    // MARK: - Basic functions
    func testValueExpiration() throws {
        let now = Int32(Date().timeIntervalSince1970)

        let store = makeStore(expiringTime: 10)
        try store.dbAccess {
            try $0.mockUnixEpochFunction(with: now)
        }
        try store.dbAccess {
            try $0.mockUnixEpochFunction(with: now + 9)
        }
        try store.set(value: "abc".data(using: .utf8),
                      forKey: "key1")
        XCTAssertEqual(try store.value(forKey: "key1"), "abc".data(using: .utf8))
        try store.dbAccess {
            try $0.mockUnixEpochFunction(with: now + 10)
        }
        XCTAssertNil(try store.value(forKey: "key1"))
    }
}

extension FileManager {
    var userDocumentsDirPath: URL {
        #if targetEnvironment(macCatalyst)
        return applicationSupportDirPath
        #else
        return FileManager.default.urls(
            for: FileManager.SearchPathDirectory.documentDirectory,
            in: FileManager.SearchPathDomainMask.userDomainMask
        )[0]
        #endif
    }

    private var applicationSupportDirPath: URL {
        FileManager.default.urls(
            for: FileManager.SearchPathDirectory.applicationSupportDirectory,
            in: FileManager.SearchPathDomainMask.userDomainMask
        )[0]
    }
}
