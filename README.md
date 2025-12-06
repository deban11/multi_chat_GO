Multi-Threaded HTTP Proxy Server with LRU Cache
Overview

This project is a high-performance HTTP Proxy Server written in C that handles multiple concurrent client connections using a thread pool architecture and implements an in-memory LRU cache protected by read–write locks for safe concurrency.

The proxy forwards client HTTP requests to remote servers, streams responses back to clients, and caches responses to reduce latency and network overhead on repeated requests.

Features

Thread Pool–based request handling (Producer–Consumer model)

Bounded job queue with condition variables

In-memory LRU cache for HTTP responses

Read–Write Lock synchronization

Concurrent readers allowed

Exclusive writes for cache insert/eviction

Deep-copy cache strategy to avoid race conditions and use-after-free bugs

Memory-safe bounded buffering

Direct socket programming (no frameworks)