LIBRARY() 
 
OWNER(somov deshevoy) 
 
PEERDIR( 
    contrib/libs/openssl 
) 
 
SRCS( 
    bio.cpp 
    x509_vfy.cpp 
) 
 
END() 
 
NEED_CHECK() 
