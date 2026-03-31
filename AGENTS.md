This file provides guidance to AI agents when working with code in this repository.

# Project

YDB is an open source Distributed SQL Database that combines high availability and scalability with strict consistency and ACID transactions. It combines row-oriented and column-oriented tables for transactional and analytical workloads, plus persistent queues (topics) for data movement.

## Build & Test

YDB uses [Yatool](https://github.com/yandex/yatool) as its build and test system (ya make). The system automatically downloads toolchains and leverages a remote build cache.

### Building

```bash
# Build server or CLI in relwithdebinfo mode (uses remote cache, recommended)
./ya make --build relwithdebinfo ydb/apps/ydbd
./ya make --build relwithdebinfo ydb/apps/ydb

# Default build (debug mode)
./ya make <folder>
```

### Testing

```bash
# Run all tests for a folder (includes build)
./ya make --build relwithdebinfo -tA <folder>

# Run specific test (supports filtering)
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*
```

**Testing conventions:**
- Tests include build step automatically
- No `-j` flag needed (handled by ya make)
- No force rebuilds required
- Use `2>&1 | tail` to limit test output
- Remote build cache (`--build relwithdebinfo`) significantly speeds up builds

## Code Architecture

### High-Level Structure

The codebase is organized around a distributed database architecture with the following major components:

**Core Engine (`ydb/core/`):**
- `base/` - Core infrastructure, actors framework, and building blocks
- `table/` - Table storage engine implementation
- `tx/` - Transaction management
- `scheme/` - Schema management (Kikimr-based)
- `blobstorage/` - Blob storage layer for data durability
- `blockstore/` - Block storage interface
- `engine/` - Query execution engine
- `driver_lib/` - Server-side driver implementation
- `persqueue/` - Persistent queue/topic implementation
- `kesus/` - Distributed mutex/lock service
- `cms/` - Cluster Management Service
- `discovery/` - Service discovery
- `security/` - Security, authentication, and authorization
- `backup/` - Backup and restore functionality

**Libraries (`ydb/library/`):**
- Reusable components and utilities shared across the codebase
- `actors/` - Actor framework implementation
- `tablet/` - Tablet execution framework
- `protobuf/` - Protocol buffer definitions
- `grpc/` - gRPC infrastructure
- `aclib/` - Access control library
- `scheme/` - Scheme types and operations
- `tx/` - Transaction library
- `persqueue/` - Persistent queue library

**Apps (`ydb/apps/`):**
- `ydbd/` - Main YDB server (database daemon)
- `ydb/` - YDB CLI (command line interface)
- `pgwire/` - PostgreSQL wire protocol proxy
- `kafka_proxy/` - Kafka API compatibility layer
- `etcd_proxy/` - etcd API compatibility layer
- `dstool/` - Administrative CLI tool for cluster operations

**Services (`ydb/services/`):**
- External-facing services and microservices
- Various specialized database services

**Utilities (`util/`, `library/`):**
- General-purpose utilities for threading, networking, memory, etc.
- Cross-platform abstractions

### Key Architectural Concepts

- **Actor Framework**: YDB uses an actor-based concurrency model throughout the codebase for handling asynchronous operations
- **Tablets**: Logical data processing units that handle specific partitions of data with their own lifecycle
- **Kikimr**: The underlying distributed system framework that powers YDB (scheme, tablet management)
- **Disaggregated Storage/Compute**: Independent horizontal scaling of storage and compute layers
- **ACID Transactions**: Distributed transaction support with strict consistency across multiple nodes

## C++

- Use C++20 or earlier
- Follow existing code style and conventions in the codebase
- The project heavily uses actors framework for concurrency

## Development Workflow

- Changes should use `ya make` for local building and testing
- Check [BUILD.md](https://github.com/ydb-platform/ydb/blob/main/BUILD.md) for detailed build instructions
- Check [CONTRIBUTING.md](https://github.com/ydb-platform/ydb/blob/main/CONTRIBUTING.md) for contribution guidelines
- See [YDB contributor documentation](https://ydb.tech/docs/en/contributor/) for more technical content
