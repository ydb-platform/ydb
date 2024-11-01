LIBRARY()

SRCS(
    yql_expr_schema.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    ydb/library/yql/ast
    yql/essentials/public/issue
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/providers/common/schema/parser
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/core
)

END()
