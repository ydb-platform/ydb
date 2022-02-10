LIBRARY() 
 
OWNER(g:kikimr)
 
PEERDIR( 
    ydb/library/mkql_proto/protos
    ydb/library/mkql_proto/ut/helpers
    ydb/public/api/protos
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
) 
 
SRCS( 
    mkql_proto.cpp 
) 
 
YQL_LAST_ABI_VERSION()

END() 

RECURSE_FOR_TESTS(
    ut
)
