GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/gtest
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    table_ut.cpp
)

END()
