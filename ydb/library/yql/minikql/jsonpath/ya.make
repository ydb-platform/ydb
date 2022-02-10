LIBRARY()

OWNER(
    g:kikimr
    g:yql
    g:yql_ydb_core
)

YQL_ABI_VERSION(
    2
    18
    0
)

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/json
    library/cpp/regex/hyperscan
    ydb/library/binary_json
    ydb/library/yql/minikql/dom
    ydb/library/yql/public/issue
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/core/issue/protos 
    ydb/library/yql/parser/proto_ast 
    ydb/library/yql/parser/proto_ast/gen/jsonpath 
)

SRCS(
    ast_builder.cpp
    ast_nodes.cpp
    binary.cpp
    executor.cpp
    jsonpath.cpp
    parse_double.cpp
    type_check.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(ast_nodes.h)

END() 
 
RECURSE_FOR_TESTS( 
    ut 
) 
