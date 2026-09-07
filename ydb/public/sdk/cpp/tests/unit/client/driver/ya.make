UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/impl/internal/sdk_runtime
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/library/grpc/client
)

SRCS(
    driver_ut.cpp
)

END()
