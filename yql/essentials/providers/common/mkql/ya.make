LIBRARY()

SRCS(
    yql_provider_mkql.cpp
    yql_provider_mkql.h
    yql_type_mkql.cpp
    yql_type_mkql.h
)

PEERDIR(
    library/cpp/json
    yql/essentials/ast
    yql/essentials/minikql
    yql/essentials/public/decimal
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/schema/expr
    yql/essentials/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()
