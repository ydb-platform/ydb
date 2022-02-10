LIBRARY() 
 
OWNER(
    g:kikimr
    g:yq
)
 
SRCS( 
    yq.cpp 
    scope.cpp
) 
 
PEERDIR( 
    library/cpp/json 
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/client/ydb_table
) 
 
END() 
