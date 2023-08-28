LIBRARY()

PEERDIR(
    contrib/libs/antlr3_cpp_runtime
    contrib/libs/protobuf
)

SRCS(
    proto_ast.cpp
)

END()

RECURSE(
    collect_issues
    gen
)
