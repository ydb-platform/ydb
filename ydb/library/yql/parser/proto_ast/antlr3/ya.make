LIBRARY()

PEERDIR(
    ydb/library/yql/parser/proto_ast
    contrib/libs/antlr3_cpp_runtime
)

SRCS(
    proto_ast_antlr3.cpp
)

END()

