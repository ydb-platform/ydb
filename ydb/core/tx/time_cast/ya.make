LIBRARY() 
 
OWNER( 
    ddoarn 
    g:kikimr
) 
 
SRCS( 
    time_cast.cpp 
) 
 
PEERDIR( 
    library/cpp/actors/core
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tx
) 
 
END() 
