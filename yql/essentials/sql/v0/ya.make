LIBRARY()

PEERDIR(
    library/cpp/charset
    library/cpp/enumbitset
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/sql/settings
    yql/essentials/core/sql_types
    yql/essentials/core/issue
    yql/essentials/core/issue/protos
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v0
    yql/essentials/sql/v0/lexer
)

SRCS(
    aggregation.cpp
    builtin.cpp
    context.cpp
    join.cpp
    insert.cpp
    list_builtin.cpp
    node.cpp
    select.cpp
    sql.cpp
    query.cpp
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(node.h)

END()

RECURSE(
    lexer
)

RECURSE_FOR_TESTS(
    ut
)
