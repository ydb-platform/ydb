UNITTEST_FOR(ydb/library/query_actor)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
)

SRCS(
    query_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
