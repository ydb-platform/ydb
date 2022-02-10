LIBRARY()

OWNER( 
    g:yql 
    g:yql_ydb_core 
) 

PEERDIR(
    contrib/libs/antlr3_cpp_runtime
    contrib/libs/protobuf
)

SRCS(
    proto_ast.cpp
)

END()

RECURSE(
    gen
    collect_issues
)
