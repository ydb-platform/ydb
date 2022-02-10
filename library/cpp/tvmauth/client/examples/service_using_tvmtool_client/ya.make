LIBRARY() 
 
OWNER(g:passport_infra) 
 
PEERDIR( 
    library/cpp/cgiparam
    library/cpp/http/simple 
    library/cpp/tvmauth/client 
) 
 
SRCS( 
    service.cpp 
) 
 
END() 
