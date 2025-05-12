LIBRARY()

PEERDIR(
    library/cpp/charset/lite
    library/cpp/enumbitset
    library/cpp/json
    library/cpp/yson/node
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/sql/settings
    yql/essentials/core/issue
    yql/essentials/core/issue/protos
    yql/essentials/core/sql_types
    yql/essentials/parser/lexer_common
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1_proto_split
    yql/essentials/parser/pg_catalog
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/proto_parser
    # for lexer tokens
    yql/essentials/parser/proto_ast/gen/v1
    yql/essentials/parser/proto_ast/gen/v1_antlr4
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
    complete
    format
    highlight
    lexer
    perf
    proto_parser
    reflect
)

RECURSE_FOR_TESTS(
    ut
    ut_antlr4
)
