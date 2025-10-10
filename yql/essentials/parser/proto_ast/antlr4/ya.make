LIBRARY()

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    yql/essentials/parser/common/antlr4
    yql/essentials/parser/proto_ast
    contrib/libs/antlr4_cpp_runtime
)

SRCS(
    proto_ast_antlr4.cpp
)

END()


