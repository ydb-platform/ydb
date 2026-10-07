UNITTEST_FOR(ydb/core/keyvalue)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/testlib/actors
)

YQL_LAST_ABI_VERSION()

SRCS(
    keyvalue_collector_ut.cpp
)

END()
