UNITTEST_FOR(ydb/public/sdk/cpp/src/client/topic)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)
TAG(ya:fat)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
ELSE()
    TIMEOUT(1200)
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
