UNITTEST_FOR(ydb/core/health_check)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    health_check_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
