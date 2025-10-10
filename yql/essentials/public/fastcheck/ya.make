LIBRARY()

SRCS(
    check_runner.cpp
    fastcheck.cpp
    linter.cpp
    lexer.cpp
    parser.cpp
    settings.cpp
    translator.cpp
    format.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/services/mounts
    yql/essentials/core/user_data
    yql/essentials/core/issue/protos
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/provider
    yql/essentials/public/langver
    yql/essentials/core/langver
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1/format
    yql/essentials/sql/settings
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/sql/v1
)

GENERATE_ENUM_SERIALIZATION(linter.h)

END()

RECURSE_FOR_TESTS(
    ut
)
