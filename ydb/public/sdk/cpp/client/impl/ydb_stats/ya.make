LIBRARY() 
 
OWNER( 
    dcherednik 
    g:kikimr 
) 
 
SRCS( 
    stats.cpp 
) 
 
PEERDIR( 
    library/cpp/grpc/client 
    library/cpp/monlib/metrics 
) 
 
END() 
