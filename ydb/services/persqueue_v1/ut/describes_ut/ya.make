UNITTEST_FOR(ydb/services/persqueue_v1)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    TIMEOUT(300)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    ic_cache_ut.cpp
    describe_topic_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/client/server
    ydb/services/persqueue_v1
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
