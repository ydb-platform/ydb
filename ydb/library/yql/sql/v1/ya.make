LIBRARY()

PEERDIR(
    library/cpp/charset
    library/cpp/enumbitset
    library/cpp/yson/node
    library/cpp/json
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/sql/settings
    ydb/library/yql/core
    ydb/library/yql/core/issue
    ydb/library/yql/core/issue/protos
    ydb/library/yql/core/sql_types
    ydb/library/yql/parser/lexer_common
    ydb/library/yql/parser/proto_ast
    ydb/library/yql/parser/proto_ast/collect_issues
    ydb/library/yql/parser/proto_ast/gen/v1
    ydb/library/yql/parser/proto_ast/gen/v1_ansi
    ydb/library/yql/parser/proto_ast/gen/v1_proto_split
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/sql/v1/lexer
    ydb/library/yql/sql/v1/proto_parser
)

SRCS(
    aggregation.cpp
    builtin.cpp
    context.cpp
    join.cpp
    insert.cpp
    list_builtin.cpp
    match_recognize.cpp
    node.cpp
    select.cpp
    source.cpp
    sql.cpp
    sql_call_expr.cpp
    sql_expression.cpp
    sql_group_by.cpp
    sql_match_recognize.cpp
    sql_into_tables.cpp
    sql_query.cpp
    sql_select.cpp
    sql_translation.cpp
    sql_values.cpp
    query.cpp
    object_processing.cpp
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(match_recognize.h)
GENERATE_ENUM_SERIALIZATION(node.h)
GENERATE_ENUM_SERIALIZATION(sql_call_param.h)

END()

RECURSE(
    format
    lexer
    perf
    proto_parser
)

RECURSE_FOR_TESTS(
    ut
)
