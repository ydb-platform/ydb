LIBRARY()

SRCS(
    obfuscate.cpp
)

PEERDIR(
    ydb/core/protos
    yql/essentials/core/sql_types
    yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4
    yql/essentials/public/issue
    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/proto_parser
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    library/cpp/json
    library/cpp/protobuf/util
    library/cpp/string_utils/base64
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
