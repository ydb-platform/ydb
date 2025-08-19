UNITTEST_FOR(ydb/library/yql/providers/generic/provider)

TAG(ya:manual)

SRCS(
    pushdown_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/random_provider
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/core/services
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/minikql
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/generic/expr_nodes
    ydb/library/yql/providers/result/provider
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/sql/pg_dummy
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
