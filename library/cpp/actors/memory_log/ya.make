LIBRARY() 
 
OWNER(
    agri
    g:kikimr
)
 
SRCS( 
    memlog.cpp 
    memlog.h 
    mmap.cpp 
) 
 
PEERDIR( 
    library/cpp/threading/queue
    contrib/libs/linuxvdso 
) 
 
END() 
