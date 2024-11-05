LIBRARY()

PEERDIR(
    ydb/library/yql/parser/proto_ast
    contrib/libs/antlr4_cpp_runtime
)

SRCS(
    proto_ast_antlr4.cpp
)

END()


