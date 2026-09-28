UNITTEST_FOR(ydb/library/ncloud/impl)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    ydb/core/base
    ydb/core/testlib
    ydb/core/testlib/actors
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
)

END()
