LIBRARY() 
 
OWNER( 
    ddoarn
    pnv1 
    g:kikimr 
) 
 
SRCS( 
    http.cpp 
    tablet_info.cpp 
    trace.cpp 
    trace_collection.cpp 
) 
 
PEERDIR( 
    ydb/core/base
    ydb/core/protos
) 
 
END() 
