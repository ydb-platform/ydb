UNITTEST_FOR(ydb/library/nebius_cloud/impl)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
)

REQUIREMENTS(ram:10)

END()
