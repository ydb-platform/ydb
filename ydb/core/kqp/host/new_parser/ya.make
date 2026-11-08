LIBRARY()

SRCS(
    kqp_ast_builder.cpp
    kqp_new_parser.cpp
    kqp_name_resolver.cpp
)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    yql/essentials/ast
    ydb/core/kqp/host/new_parser/grammar/default
    ydb/core/kqp/host/new_parser/grammar/ansi
    yql/essentials/parser/common/antlr4
    yql/essentials/public/issue
    yql/essentials/sql/settings
)

END()
