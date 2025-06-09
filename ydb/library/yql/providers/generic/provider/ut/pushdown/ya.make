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
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/services
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/providers/result/provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/re2
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
