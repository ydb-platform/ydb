UNITTEST_FOR(ydb/library/yql/providers/generic/provider)

SRCS(
    pushdown_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/random_provider
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/generic/expr_nodes
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/core/services
    ydb/library/yql/minikql
    ydb/library/yql/minikql/invoke_builtins/llvm16
    ydb/library/yql/providers/result/provider
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/udfs/common/re2
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
