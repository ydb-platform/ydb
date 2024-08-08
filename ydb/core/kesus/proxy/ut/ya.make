UNITTEST_FOR(ydb/core/kesus/proxy)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    proxy_actor_ut.cpp
    ut_helpers.cpp
)

YQL_LAST_ABI_VERSION()

END()
