UNITTEST_FOR(ydb/library/ncloud/impl)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/testlib
    ydb/core/testlib/actors
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
)

END()
