LIBRARY()

SRCS(
    yql_expr_schema.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/ast
    yql/essentials/public/issue
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/providers/common/schema/parser
    yql/essentials/parser/pg_catalog
    yql/essentials/core
)

END()
