LIBRARY()

SRCS(
    kqp_ast_builder.cpp
    kqp_new_parser.cpp
    kqp_name_resolver.cpp
    kqp_expr_lowering.cpp
    kqp_type_annotation.cpp
)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    yql/essentials/ast
    yql/essentials/core/sql_types
    yql/essentials/core/type_ann
    yql/essentials/public/udf
    ydb/core/kqp/host/new_parser/grammar/default
    ydb/core/kqp/host/new_parser/grammar/ansi
    yql/essentials/parser/common/antlr4
    yql/essentials/public/issue
    yql/essentials/sql/settings
)

YQL_LAST_ABI_VERSION()

END()
