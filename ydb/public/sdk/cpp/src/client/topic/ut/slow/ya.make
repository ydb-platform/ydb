UNITTEST_FOR(ydb/public/sdk/cpp/src/client/topic)

REQUIREMENTS(ram:32 cpu:4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    txusage_slow_ut.cpp
)

END()
