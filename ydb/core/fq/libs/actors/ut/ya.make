UNITTEST_FOR(ydb/core/fq/libs/actors)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/retry
    library/cpp/testing/unittest
    ydb/core/fq/libs/actors
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    database_resolver_ut.cpp
)

END()
