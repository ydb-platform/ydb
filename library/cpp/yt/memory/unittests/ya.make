GTEST(unittester-library-memory)

OWNER(g:yt)

IF (NOT OS_WINDOWS) 
    ALLOCATOR(YT) 
ENDIF() 

SRCS(
    intrusive_ptr_ut.cpp
    weak_ptr_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/memory
)

END()
