GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    metrics_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/impl/stats
    ydb/public/sdk/cpp/src/client/metrics
    ydb/public/sdk/cpp/src/client/query/impl
    ydb/public/sdk/cpp/src/client/table/impl
)

END()
