LIBRARY()

SRCS(
    fastcheck.cpp
    linter.cpp
    lexer.cpp
    parser.cpp
    translator.cpp
    format.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/services/mounts
    yql/essentials/core/user_data
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/providers/common/provider
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/proto_parser
    yql/essentials/sql/v1/format
    yql/essentials/sql/settings
    yql/essentials/parser/pg_wrapper/interface
)

END()

RECURSE_FOR_TESTS(
    ut
)
