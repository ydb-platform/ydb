GTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

SRCS(
    query_spans_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    ydb/public/sdk/cpp/src/client/impl/observability
)

END()
