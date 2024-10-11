import XCTest
@testable import SQLiteKeyValueStore

final class SQLiteKeyValueStoreTests: XCTestCase {

    override func setUp() {
        super.setUp()
        try? FileManager.default.removeItem(at: FileManager.default.userDocumentsDirPath.appendingPathComponent("testStore"))
    }

    func makeStore(name: String = "testStore") -> SQLiteKeyValueStore {
        return try! .init(
            logger: { print("[\($1)] \(Date()) \(String(describing: $0)) \($2)") },
            rootPath: FileManager.default.userDocumentsDirPath,
            storeName: name,
            accessQueueQoS: .userInitiated
        )
    }

    // MARK: - Basic functions
    func testSetValueForKey() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1", inNamespace: store.defaultNamespace)
        XCTAssertEqual(try store.value(forKey: "key1", inNamespace: store.defaultNamespace), "abc".data(using: .utf8))
    }

    func testSetValuesForKeys() throws {
        let store = makeStore()
        try store.set(valuesForKeys: [
            "key1":"abc".data(using: .utf8)!,
            "key2":"def".data(using: .utf8)!
        ], inNamespace: store.defaultNamespace)
        XCTAssertEqual(try store.value(forKey: "key1", inNamespace: store.defaultNamespace), "abc".data(using: .utf8))
        XCTAssertEqual(try store.value(forKey: "key2", inNamespace: store.defaultNamespace), "def".data(using: .utf8))
    }

    func testGetValuesStartingWithKeyPrefix() throws {
        let prefix = "prefix_"
        let prefixedKey1 = "\(prefix)key1"
        let prefixedKey2 = "\(prefix)key2"
        let unprefixedKey = "key3"

        let store = makeStore()
        try store.set(value: "value1".data(using: .utf8)!, forKey: prefixedKey1, inNamespace: store.defaultNamespace)
        try store.set(value: "value2".data(using: .utf8)!, forKey: prefixedKey2, inNamespace: store.defaultNamespace)
        try store.set(value: "value3".data(using: .utf8)!, forKey: unprefixedKey, inNamespace: store.defaultNamespace)

        let values = try store.values(forKeysStartingWith: prefix, inNamespace: store.defaultNamespace, parentAccess: nil)
        XCTAssertEqual(values.count, 2)
        XCTAssertTrue(values.contains(where: { $0.key == prefixedKey1 }))
        XCTAssertTrue(values.contains(where: { $0.key == prefixedKey2 }))
        XCTAssertFalse(values.contains(where: { $0.key == unprefixedKey }))
    }

    func testDeleteValuesStartingWithKeyPrefix() throws {
        let prefix = "prefix_"
        let prefixedKey1 = "\(prefix)key1"
        let prefixedKey2 = "\(prefix)key2"
        let unprefixedKey = "key3"

        let store = makeStore()
        try store.set(value: "value1".data(using: .utf8)!, forKey: prefixedKey1, inNamespace: store.defaultNamespace)
        try store.set(value: "value2".data(using: .utf8)!, forKey: prefixedKey2, inNamespace: store.defaultNamespace)
        try store.set(value: "value3".data(using: .utf8)!, forKey: unprefixedKey, inNamespace: store.defaultNamespace)

        try store.delete(valuesForKeysStartingWith: prefix, inNamespace: store.defaultNamespace, parentAccess: nil)
        XCTAssertNil(try store.value(forKey: prefixedKey1, inNamespace: store.defaultNamespace))
        XCTAssertNil(try store.value(forKey: prefixedKey2, inNamespace: store.defaultNamespace))
        XCTAssertNotNil(try store.value(forKey: unprefixedKey, inNamespace: store.defaultNamespace))
    }

    func testDeleteValue() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1", inNamespace: store.defaultNamespace)
        XCTAssertEqual(try store.value(forKey: "key1", inNamespace: store.defaultNamespace), "abc".data(using: .utf8))

        try store.delete(valuesForKeys: ["key1"], inNamespace: store.defaultNamespace)
        XCTAssertNil(try store.value(forKey: "key1", inNamespace: store.defaultNamespace))
    }

    func testDeleteValueThroughNilUpdate() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1", inNamespace: store.defaultNamespace)
        XCTAssertEqual(try store.value(forKey: "key1", inNamespace: store.defaultNamespace), "abc".data(using: .utf8))

        try store.performUpdateInSingleDbAccess(forKey: "key1", inNamespace: store.defaultNamespace) {
            $0 = nil
        }
        XCTAssertNil(try store.value(forKey: "key1", inNamespace: store.defaultNamespace))
    }

    func testOpenExistingStore() throws {

        func generateData() throws {
            let store = makeStore()
            try store.set(value: "abc".data(using: .utf8)!,
                          forKey: "key1", inNamespace: store.defaultNamespace)
        }
        try generateData()

        // now open another store, pointing on the same file, and query it.
        let store = makeStore()
        XCTAssertEqual(
            try store.value(forKey: "key1", inNamespace: store.defaultNamespace),
            "abc".data(using: .utf8)!,
            store.defaultNamespace
        )
    }

    // MARK - namespaces functions
    func testSameKeyDifferentNamespaces() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns1")

        try store.set(value: "def".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns2")

        XCTAssertEqual(try store.value(forKey: "key1", inNamespace: "ns1"), "abc".data(using: .utf8))
        XCTAssertEqual(try store.value(forKey: "key1", inNamespace: "ns2"), "def".data(using: .utf8))
    }

    func testListNamespaces() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns1")

        try store.set(value: "def".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns2")

        XCTAssertEqual(try store.listNamespaces(), ["ns1" , "ns2"])
    }

    func testDeleteAllValuesInNamespaces() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns1")

        try store.set(value: "def".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns2")

        XCTAssertEqual(try store.listNamespaces(), ["ns1" , "ns2"])
        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns2", resultRange: 0...100)).count, 1)
        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns1", resultRange: 0...100)).count, 1)

        try store.delete(allValuesInNamespace: "ns1")
        XCTAssertEqual(try store.listNamespaces(), ["ns1" , "ns2"])

        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns2", resultRange: 0...100)).count, 1)
        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns1", resultRange: 0...100)).count, 0)
    }

    func testDeleteNamespace() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns1")

        try store.set(value: "def".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns2")

        XCTAssertEqual(try store.listNamespaces(), ["ns1" , "ns2"])
        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns2", resultRange: 0...100)).count, 1)
        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns1", resultRange: 0...100)).count, 1)

        try store.delete(namespace: "ns1")
        XCTAssertEqual(try store.listNamespaces(), ["ns2"]) // note that we need to make the test here, as listing keys operations below will recreate the namespace as a sideeffect

        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns2", resultRange: 0...100)).count, 1)
        XCTAssertEqual((try store.listKeysAndValues(inNamespace:"ns1", resultRange: 0...100)).count, 0)
    }

    func testDeleteAllNamespaces() throws {
        let store = makeStore()
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns1")

        try store.set(value: "def".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns2")

        try store.deleteAllNamespaces()
        XCTAssertEqual(try store.listNamespaces().count, 0)
    }

    func testCountKeysPerNamespace() throws {
        let store = makeStore()
        try store.set(valuesForKeys: ["k1" : Data(), "k2": Data(), "k3": Data()], inNamespace: "ns1")
        try store.set(value: "def".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: "ns2")

        let nbKeysPerNamespace = try store.numberOfKeysPerNamespace()
        XCTAssertEqual(nbKeysPerNamespace.keys.count, 2)
        XCTAssertEqual(nbKeysPerNamespace["ns1"], 3)
        XCTAssertEqual(nbKeysPerNamespace["ns2"], 1)
    }

    func testOpenWALMode() throws {
        let store = try! SQLiteKeyValueStore(
            logger: { print("[\($1)] \(Date()) \(String(describing: $0)) \($2)") },
            rootPath: FileManager.default.userDocumentsDirPath,
            storeName: "db",
            accessQueueQoS: .userInitiated,
            journalMode: .wal
        )

        //check for "wal" file
        let walFilePath = FileManager.default.userDocumentsDirPath.appendingPathComponent("db-wal")
        XCTAssert(FileManager.default.fileExists(atPath: walFilePath.path))
    }

    func testSpecialTableName() throws {
        let store = makeStore()
        let funkyNamespace = "34243-34349&é''"
        try store.set(value: "abc".data(using: .utf8)!,
                      forKey: "key1",
                      inNamespace: funkyNamespace)

        XCTAssertEqual(try store.value(forKey: "key1", inNamespace: funkyNamespace), "abc".data(using: .utf8))
    }

    func testListAllKeysAndValues() throws {
        let store = makeStore()
        for i in 0..<50 {
            try store.set(value: "v\(i)".data(using: .ascii)!, forKey: "\(i)", inNamespace: store.defaultNamespace)
        }
        let all = try store.listAllKeysAndValues(inNamespace: store.defaultNamespace, batchSize: 1).sorted(by: { kv1, kv2 in
            return kv1.0.compare(kv2.0, options: .numeric) == .orderedAscending
        })

        guard all.count == 50 else {
            XCTFail("unexpected number of result : \(all.count), expected 50")
            return
        }
        for i in 0..<50 {
            let kv = all[i]
            XCTAssertEqual(kv.0, "\(i)")
            XCTAssertEqual(kv.1, "v\(i)".data(using: .ascii))
        }

    }
}
