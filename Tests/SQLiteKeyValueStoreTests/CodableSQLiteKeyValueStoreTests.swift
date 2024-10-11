//
//  File.swift
//  
//
//  Created by Benjamin Garrigues on 02/09/2022.
//

import Foundation
import XCTest
@testable import SQLiteKeyValueStore

final class CodableSQLiteKeyValueStoreTests: XCTestCase {

    let nameSpace = "mock"


    struct CodableBlob: Codable, Equatable {
        var text: String
    }


    /*override func setUp() {
        super.setUp()
        FileManager.default.cleanTemporaryDirectoryContent()
        FileManager.default.cleanAllUserDirectoryContent()
    }*/

    func makeStore(name: String = "testStore") -> CodableSQLiteKeyValueStore {
        return try! .init(
            logger: { print("[\($1)] \(Date()) \(String(describing: $0)) \($2)") },
            rootPath: FileManager.default.userDocumentsDirPath,
            storeName: name,
            accessQueueQoS: .userInitiated
        )
    }

    func testSetCodable() throws {
        let store = makeStore()
        let blob = CodableBlob(text: "abc")
        try store.setCodable(value: blob, forKey: "blob1", inNamespace: nameSpace)
        XCTAssertEqual(try store.valueCodable(forKey: "blob1", inNamespace: nameSpace), blob)
    }

    func testAtomicUpdateCodable() throws {
        let store = makeStore()
        var blob = CodableBlob(text: "abc")
        try store.performCodableUpdateInSingleDbAccess(forKey: "blob1", update: { val in
            val = blob
        })
        XCTAssertEqual((try store.valueCodable(forKey: "blob1", inNamespace: store.defaultNamespace) as CodableBlob?)?.text, "abc")

        blob.text = "def"
        try store.performCodableUpdateInSingleDbAccess(forKey: "blob1", update: { val in
            val = blob
        })
        XCTAssertEqual((try store.valueCodable(forKey: "blob1", inNamespace: store.defaultNamespace) as CodableBlob?)?.text, "def")

    }

    func testValuesCodable() throws {
        let store = makeStore()
        let (key1, val1) = ("key1", "val1")
        let (key2, val2) = ("key2", "val1")

        try store.setCodable(value: CodableBlob(text: val1), forKey: key1)
        try store.setCodable(value: CodableBlob(text: val2), forKey: key2)

        let keysAndValues: [String: CodableBlob] = try store.valuesCodable(
            forKeys: [key1, key2],
            inNamespace: store.defaultNamespace
        )

        XCTAssertEqual(keysAndValues[key1]?.text, val1)
        XCTAssertEqual(keysAndValues[key2]?.text, val2)
    }
}
