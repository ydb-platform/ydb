UNITTEST_FOR(ydb/library/yql/core)

SIZE(SMALL)

SRCS(
    ../yql_opt_utils_ut.cpp
)

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/public/udf/service/terminate_policy
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/ast
    ydb/library/yql/sql
    ydb/library/yql/sql/v1
)

YQL_LAST_ABI_VERSION()

END()
