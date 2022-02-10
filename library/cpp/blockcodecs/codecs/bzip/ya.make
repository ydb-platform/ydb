LIBRARY() 
 
OWNER(pg) 
 
PEERDIR( 
    contrib/libs/libbz2 
    library/cpp/blockcodecs/core
) 
 
SRCS( 
    GLOBAL bzip.cpp 
) 
 
END() 
