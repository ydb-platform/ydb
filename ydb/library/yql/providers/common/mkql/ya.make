LIBRARY()

SRCS(
    parser.cpp
    parser.h
    yql_provider_mkql.cpp
    yql_provider_mkql.h
    yql_type_mkql.cpp
    yql_type_mkql.h
)

PEERDIR(
    library/cpp/json
    ydb/library/yql/ast
    ydb/library/yql/minikql
    ydb/library/yql/public/decimal
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()
