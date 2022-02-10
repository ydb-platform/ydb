LIBRARY() 
 
OWNER( 
    g:kikimr 
    g:logbroker 
) 
 
SRCS( 
    pqv1.cpp 
) 
 
PEERDIR( 
    ydb/public/api/protos 
    ydb/public/sdk/cpp/client/ydb_persqueue_core 
) 
 
END() 
