UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_ERASURE=block_4-2)

# Test with specific gRPC services enabled to ensure compression testing
ENV(YDB_GRPC_SERVICES=table)

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/scheme
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/public/api/grpc
    ydb/tests/library/harness
    contrib/libs/grpc
)

SRCS(
    grpc_compression_advanced_test.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:24 cpu:4)
ELSE()
    REQUIREMENTS(ram:16 cpu:2)
ENDIF()

END()