import XCTest
import NIO

@testable import SwiftDataLoader

/// Primary API
final class DataLoaderTests: XCTestCase {

    /// Builds a really really simple data loader'
    func testReallyReallySimpleDataLoader() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }
        
        let identityLoader = DataLoader<Int, Int>(options: DataLoaderOptions(batchingEnabled: false), batchLoadFunction: { (keys) in
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let value = try identityLoader.load(key: 1)

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value.wait() == 1)
    }

    /// Supports loading multiple keys in one call
    func testLoadingMultipleKeys() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }
        
        let identityLoader = DataLoader<Int, Int>(batchLoadFunction: { (keys) in
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let values = try identityLoader.loadMany(keys: [1, 2], on: eventLoopGroup)

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try values.wait() == [1,2])

        let empty = try identityLoader.loadMany(keys: [], on: eventLoopGroup).wait()

        XCTAssertTrue(empty.isEmpty)
    }

    // Batches multiple requests
    func testMultipleRequests() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[Int]]()
        
        let identityLoader = DataLoader<Int, Int>(options: DataLoaderOptions(batchingEnabled: true), batchLoadFunction: { (keys) in
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let value1 = try identityLoader.load(key: 1)
        let value2 = try identityLoader.load(key: 2)

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.map { $0 }.wait() == 1)
        XCTAssertTrue(try value2.map { $0 }.wait() == 2)

        XCTAssertTrue(loadCalls == [[1], [2]])
    }

    /// Batches multiple requests with max batch sizes
    func testMultipleRequestsWithMaxBatchSize() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[Int]]()
            
        let identityLoader = DataLoader<Int, Int>(options: DataLoaderOptions(batchingEnabled: true, maxBatchSize: 2), batchLoadFunction: { (keys) in
            
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let value1 = try identityLoader.load(key: 1 )
        let value2 = try identityLoader.load(key: 2 )
        let value3 = try identityLoader.load(key: 3 )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.map { $0 }.wait() == 1)
        XCTAssertTrue(try value2.map { $0 }.wait() == 2)
        XCTAssertTrue(try value3.map { $0 }.wait() == 3)

        XCTAssertTrue(loadCalls == [[1,2], [3]])
    }

    /// Coalesces identical requests
    func testCoalescesIdenticalRequests() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[Int]]()
        
        let identityLoader = DataLoader<Int, Int>(batchLoadFunction: { (keys) in
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let value1 = try identityLoader.load(key: 1)
        let value2 = try identityLoader.load(key: 1)

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.map { $0 }.wait() == 1)
        XCTAssertTrue(try value2.map { $0 }.wait() == 1)

        XCTAssertTrue(loadCalls == [[1]])
    }

    // Caches repeated requests
    func testCachesRepeatedRequests() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[String]]()
        
        let identityLoader = DataLoader<String, String>(options: DataLoaderOptions(), batchLoadFunction: { (keys) in
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let value1 = try identityLoader.load(key: "A")
        let value2 = try identityLoader.load(key: "B")

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.wait() == "A")
        XCTAssertTrue(try value2.wait() == "B")
        XCTAssertTrue(loadCalls == [["A", "B"]])

        let value3 = try identityLoader.load(key: "A")
        let value4 = try identityLoader.load(key: "C")

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value3.wait() == "A")
        XCTAssertTrue(try value4.wait() == "C")
        XCTAssertTrue(loadCalls == [["A", "B"], ["C"]])

        let value5 = try identityLoader.load(key: "A")
        let value6 = try identityLoader.load(key: "B")
        let value7 = try identityLoader.load(key: "C")


        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value5.wait() == "A")
        XCTAssertTrue(try value6.wait() == "B")
        XCTAssertTrue(try value7.wait() == "C")
        XCTAssertTrue(loadCalls == [["A", "B"], ["C"]])
    }

    /// Clears single value in loader
    func testClearSingleValueLoader() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[String]]()
        
        let identityLoader = DataLoader<String, String>(options: DataLoaderOptions(), batchLoadFunction: { (keys) in
            
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let value1 = try identityLoader.load(key: "A" )
        let value2 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.wait() == "A")
        XCTAssertTrue(try value2.wait() == "B")
        XCTAssertTrue(loadCalls == [["A", "B"]])

        _ = identityLoader.clear(key: "A")

        let value3 = try identityLoader.load(key: "A" )
        let value4 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value3.wait() == "A")
        XCTAssertTrue(try value4.wait() == "B")
        XCTAssertTrue(loadCalls == [["A", "B"], ["A"]])
    }

    /// Clears all values in loader
    func testClearsAllValuesInLoader() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[String]]()
        
        let identityLoader = DataLoader<String, String>(options: DataLoaderOptions(), batchLoadFunction: { (keys) in
            
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        let value1 = try identityLoader.load(key: "A" )
        let value2 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.wait() == "A")
        XCTAssertTrue(try value2.wait() == "B")
        XCTAssertTrue(loadCalls == [["A", "B"]])

        _ = identityLoader.clearAll()

        let value3 = try identityLoader.load(key: "A" )
        let value4 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value3.wait() == "A")
        XCTAssertTrue(try value4.wait() == "B")
        XCTAssertTrue(loadCalls == [["A", "B"], ["A", "B"]])
    }

    // Allows priming the cache
    func testAllowsPrimingTheCache() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[String]]()
        
        let identityLoader = DataLoader<String, String>(options: DataLoaderOptions(), batchLoadFunction: { (keys) in
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        _ = identityLoader.prime(key: "A", value: "A", on: eventLoopGroup)

        let value1 = try identityLoader.load(key: "A")
        let value2 = try identityLoader.load(key: "B")

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.wait() == "A")
        XCTAssertTrue(try value2.wait() == "B")
        XCTAssertTrue(loadCalls == [["B"]])
    }

    /// Does not prime keys that already exist
    func testDoesNotPrimeKeysThatAlreadyExist() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[String]]()
        
        let identityLoader = DataLoader<String, String>(options: DataLoaderOptions(), batchLoadFunction: { (keys) in
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        _ = identityLoader.prime(key: "A", value: "X", on: eventLoopGroup )

        let value1 = try identityLoader.load(key: "A" )
        let value2 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.wait() == "X")
        XCTAssertTrue(try value2.wait() == "B")

        _ = identityLoader.prime(key: "A", value: "Y", on: eventLoopGroup )
        _ = identityLoader.prime(key: "B", value: "Y", on: eventLoopGroup )

        let value3 = try identityLoader.load(key: "A" )
        let value4 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value3.wait() == "X")
        XCTAssertTrue(try value4.wait() == "B")

        XCTAssertTrue(loadCalls == [["B"]])
    }

    /// Allows forcefully priming the cache
    func testAllowsForcefullyPrimingTheCache() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully())
        }

        var loadCalls = [[String]]()
        
        let identityLoader = DataLoader<String, String>(options: DataLoaderOptions(), batchLoadFunction: { (keys) in
            loadCalls.append(keys)
            let results = keys.map { DataLoaderFutureValue.success($0) }

            return eventLoopGroup.next().makeSucceededFuture(results)
        }, eventLoop: eventLoopGroup.next())

        _ = identityLoader.prime(key: "A", value: "X", on: eventLoopGroup)

        let value1 = try identityLoader.load(key: "A" )
        let value2 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value1.wait() == "X")
        XCTAssertTrue(try value2.wait() == "B")

        _ = identityLoader.clear(key: "A").prime(key: "A", value: "Y", on: eventLoopGroup )
        _ = identityLoader.clear(key: "B").prime(key: "B", value: "Y", on: eventLoopGroup)

        let value3 = try identityLoader.load(key: "A" )
        let value4 = try identityLoader.load(key: "B" )

        XCTAssertNoThrow(try identityLoader.dispatchQueue())

        XCTAssertTrue(try value3.wait() == "Y")
        XCTAssertTrue(try value4.wait() == "Y")

        XCTAssertTrue(loadCalls == [["B"]])
    }

    static var allTests: [(String, (DataLoaderTests) -> () throws -> Void)] = [
        ("testRealyRealySimpleDataLoader", testReallyReallySimpleDataLoader),
        ("testLoadingMultipleKeys", testLoadingMultipleKeys),
        ("testMultipleRequests", testMultipleRequests),
        ("testMultipleRequestsWithMaxBatchSize", testMultipleRequestsWithMaxBatchSize),
        ("testCoalescesIdenticalRequests", testCoalescesIdenticalRequests),
        ("testCachesRepeatedRequests", testCachesRepeatedRequests),
        ("testClearSingleValueLoader", testClearSingleValueLoader),
        ("testClearsAllValuesInLoader", testClearsAllValuesInLoader),
        ("testAllowsPrimingTheCache", testAllowsPrimingTheCache),
        ("testDoesNotPrimeKeysThatAlreadyExist", testDoesNotPrimeKeysThatAlreadyExist),
        ("testAllowsForcefullyPrimingTheCache", testAllowsForcefullyPrimingTheCache)
    ]
}
