UNITTEST_FOR(ydb/core/tx/limiter/grouped_memory)

SIZE(SMALL)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    ydb/core/tx/limiter/grouped_memory/usage
    yql/essentials/public/udf/service/stub
    yql/essentials/parser/pg_wrapper
)

SRCS(
    ut_manager.cpp
)

YQL_LAST_ABI_VERSION()

END()
