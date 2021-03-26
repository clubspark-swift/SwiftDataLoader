//
//  DataLoader.swift
//  App
//
//  Created by Kim de Vos on 01/06/2018.
//
import NIO

public enum DataLoaderFutureValue<T> {
    case success(T)
    case failure(Error)
}

public typealias BatchLoadFunction<Key, Value> = (_ keys: [Key]) -> EventLoopFuture<[DataLoaderFutureValue<Value>]>

// Private
private typealias LoaderQueue<Key, Value> = Array<(key: Key, promise: EventLoopPromise<Value>)>

final public class DataLoader<Key: Hashable, Value> {

    private let batchLoadFunction: BatchLoadFunction<Key, Value>
    private let options: DataLoaderOptions<Key, Value>

    private var futureCache = [Key: EventLoopFuture<Value>]()
    private var queue = LoaderQueue<Key, Value>()

    private let eventLoop: EventLoop
    private var loading = false
    
    public init(options: DataLoaderOptions<Key, Value> = DataLoaderOptions(),
                batchLoadFunction: @escaping BatchLoadFunction<Key, Value>,
                eventLoop: EventLoop) {
        self.options = options
        self.batchLoadFunction = batchLoadFunction
        self.eventLoop = eventLoop
    }


    /// Loads a key, returning a `Promise` for the value represented by that key.
    public func load(key: Key) -> EventLoopFuture<Value> {
        let cacheKey = options.cacheKeyFunction?(key) ?? key

        if options.cachingEnabled, let cachedFuture = futureCache[cacheKey] {
            return cachedFuture
        }

        let promise: EventLoopPromise<Value> = eventLoop.next().makePromise()

        if options.batchingEnabled {
            queue.append((key: key, promise: promise))
            if !self.loading {
                self.dispatchQueue()
            }
        } else {
            _ = batchLoadFunction([key]).map { results  in
                if results.isEmpty {
                    promise.fail(DataLoaderError.noValueForKey("Did not return value for key: \(key)"))
                } else {
                    let result = results[0]
                    switch result {
                    case .success(let value): promise.succeed(value)
                    case .failure(let error): promise.fail(error)
                    }
                }
            }
        }

        let future = promise.futureResult

        if options.cachingEnabled {
            futureCache[cacheKey] = future
        }

        return future
    }

    public func loadMany(keys: [Key], on eventLoop: EventLoopGroup) throws -> EventLoopFuture<[Value]> {
        guard !keys.isEmpty else { return eventLoop.next().makeSucceededFuture([]) }

        let promise: EventLoopPromise<[Value]> = eventLoop.next().makePromise()

        var result = [Value]()

        let futures = keys.map { load(key: $0) }

        for future in futures {
            _ = future.map { value in
                result.append(value)

                if result.count == keys.count {
                    promise.succeed(result)
                }
            }
        }

        return promise.futureResult
    }

    public func clear(key: Key) -> DataLoader<Key, Value> {
        let cacheKey = options.cacheKeyFunction?(key) ?? key
        futureCache.removeValue(forKey: cacheKey)
        return self
    }

    func clearAll() -> DataLoader<Key, Value> {
        futureCache.removeAll()
        return self
    }

    func prime(key: Key, value: Value, on eventLoop: EventLoopGroup) -> DataLoader<Key, Value> {
        let cacheKey = options.cacheKeyFunction?(key) ?? key

        if futureCache[cacheKey] == nil {
            let promise: EventLoopPromise<Value> = eventLoop.next().makePromise()
            promise.succeed(value)

            futureCache[cacheKey] = promise.futureResult
        }

        return self
    }

    // MARK: - Private
    private func dispatchQueueBatch(queue: LoaderQueue<Key, Value>) { //} throws { //}-> EventLoopFuture<[Value]> {
        let keys = queue.map { $0.key }

        if keys.isEmpty {
            return
        }

        // Step through the values, resolving or rejecting each Promise in the
        // loaded queue.
        self.loading = true
            _ = batchLoadFunction(keys)
            .flatMapThrowing { values in
                self.loading = false
                if values.count != keys.count {
                    throw DataLoaderError.typeError("The function did not return an array of the same length as the array of keys. \nKeys count: \(keys.count)\nValues count: \(values.count)")
                }

                for entry in queue.enumerated() {
                    let result = values[entry.offset]

                    switch result {
                    case .failure(let error): entry.element.promise.fail(error)
                    case .success(let value): entry.element.promise.succeed(value)
                    }
                }
                self.dispatchQueue()
            }
            .flatMapError { error in
                self.failedDispatch(queue: queue, error: error)
                self.dispatchQueue()
                return self.eventLoop.makeSucceededFuture(Void())
        }
    }

    internal func dispatchQueue() {
        // Take the current loader queue, replacing it with an empty queue.
        let queue = self.queue
        //self.queue = []
        
        // If a maxBatchSize was provided and the queue is longer, then segment the
        // queue into multiple batches, otherwise treat the queue as a single batch.
        if let maxBatchSize = options.maxBatchSize, maxBatchSize > 0 && maxBatchSize < queue.count {
            //for i in 0...(queue.count / maxBatchSize) {
            let startIndex = 0// i * maxBatchSize
            let endIndex = maxBatchSize
            let slicedQueue = queue[startIndex..<min(endIndex, queue.count)]
            self.queue = Array(queue[endIndex..<queue.count])
            dispatchQueueBatch(queue: Array(slicedQueue))
            //}
        } else {
            self.queue = []
            dispatchQueueBatch(queue: queue)
        }
    }

    private func failedDispatch(queue: LoaderQueue<Key, Value>, error: Error) {
        queue.forEach { (key, promise) in
            _ = clear(key: key)
            promise.fail(error)
        }
    }
}
