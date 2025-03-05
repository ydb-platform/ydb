UNITTEST_FOR(ydb/services/persqueue_v1)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ic_cache_ut.cpp
    describe_topic_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/client/server
    ydb/services/persqueue_v1
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/api/grpc
)

YQL_LAST_ABI_VERSION()

END()
