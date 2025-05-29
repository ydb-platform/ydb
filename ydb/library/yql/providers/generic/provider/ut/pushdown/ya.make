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
    yql/library/yql/ast
    yql/library/yql/core
    yql/library/yql/core/services
    yql/library/yql/minikql
    yql/library/yql/minikql/invoke_builtins/llvm16
    yql/library/yql/providers/result/provider
    yql/library/yql/public/udf/service/stub
    yql/library/yql/sql
    yql/library/yql/sql/pg_dummy
    yql/library/yql/udfs/common/re2
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
