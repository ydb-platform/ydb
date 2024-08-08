UNITTEST_FOR(ydb/core/persqueue)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(300)
ELSE()
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    TIMEOUT(60)
ENDIF()

PEERDIR(
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    caching_proxy_ut.cpp
)

# RESOURCE(
# )

END()
