LIBRARY()

PEERDIR(
    yql/essentials/parser/common/antlr4
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    contrib/libs/antlr4_cpp_runtime
)

SRCS(
    base_visitor.cpp
    cursor_text.cpp
    narrowing_visitor.cpp
    parse_tree.cpp
    parser.cpp
    path_visitor.cpp
)

END()

RECURSE(
    benchmark
)
