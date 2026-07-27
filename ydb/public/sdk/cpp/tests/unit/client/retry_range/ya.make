UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    retry_range_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/row_ranges
    ydb/public/sdk/cpp/src/client/table
)

END()
