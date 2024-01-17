LIBRARY()

PEERDIR(
    library/cpp/charset
    library/cpp/enumbitset
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/sql/settings
    ydb/library/yql/core
    ydb/library/yql/core/issue
    ydb/library/yql/core/issue/protos
    ydb/library/yql/parser/proto_ast
    ydb/library/yql/parser/proto_ast/collect_issues
    ydb/library/yql/parser/proto_ast/gen/v0
    ydb/library/yql/parser/proto_ast/gen/v0_proto_split
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
