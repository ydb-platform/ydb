# gRPC Server Side Compression Functional Tests

This directory contains functional tests for gRPC server-side compression support in YDB.

## Test Files

### 1. `grpc_compression_test.cpp` (ya.make)
Basic functional tests that verify:
- YDB server can handle gRPC clients with compression enabled
- Different compression algorithms (GZIP, DEFLATE, NONE) work correctly
- Large request/response compression functionality
- Data integrity is maintained with compression

### 2. `grpc_compression_advanced_test.cpp` (advanced_ya.make)
Advanced tests that include:
- Compression effectiveness measurement
- Different compression levels testing
- Mixed compression scenarios
- Concurrent clients with different compression settings
- Performance characteristics testing

### 3. `grpc_compression_server_test.cpp` (server_ya.make)
Server-side compression configuration tests:
- Server compression response handling
- Data transfer with compression
- Data integrity with various character sets
- Performance characteristics with different data types

## Configuration Files

### `test_config_with_compression.yaml`
Example YDB configuration that enables server-side compression with:
- GZIP compression algorithm
- Medium compression level
- Table, legacy, scripting, and discovery services enabled

## Running the Tests

Each test can be run independently using the Ya build system:

```bash
# Basic compression tests
ya make -A ydb/tests/functional/grpc_compression

# Advanced compression tests  
ya make -A ydb/tests/functional/grpc_compression:advanced

# Server compression tests
ya make -A ydb/tests/functional/grpc_compression:server
```

## Test Coverage

The tests verify:

1. **Client-side compression**: Clients can send compressed requests
2. **Server-side handling**: Server properly handles compressed requests
3. **Response compression**: Server can send compressed responses
4. **Multiple algorithms**: GZIP, DEFLATE, and no compression
5. **Data integrity**: Compression doesn't corrupt data
6. **Performance**: Large data sets benefit from compression
7. **Concurrency**: Multiple clients with different compression settings
8. **Unicode support**: UTF-8 data is correctly compressed/decompressed

## Environment Variables

The tests can be configured using these environment variables:
- `YDB_GRPC_SERVICES`: Comma-separated list of gRPC services to enable
- `YDB_DATABASE`: Database path for testing
- `YDB_ENDPOINT`: YDB server endpoint
- `YDB_TOKEN`: Authentication token (if required)

## Implementation Notes

- Tests use the YDB recipe system to start a test YDB instance
- gRPC compression is configured at the channel and call level
- Tests validate both functional correctness and basic performance characteristics
- Error handling includes authentication errors (which may occur in test environments)