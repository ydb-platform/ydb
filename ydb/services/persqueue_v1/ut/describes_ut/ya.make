UNITTEST_FOR(ydb/services/persqueue_v1)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ic_cache_ut.cpp
    describe_topic_ut.cpp
)

PEERDIR(
    ydb/core/client/server
    ydb/core/persqueue/public/describer
    ydb/core/testlib/default
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
