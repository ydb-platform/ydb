LIBRARY() 
 
OWNER(
    agri
    g:kikimr
)
 
SRCS( 
    tag.cpp 
) 
 
PEERDIR( 
    library/cpp/charset
    library/cpp/containers/atomizer
) 
 
IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
    PEERDIR(
        library/cpp/malloc/api
        library/cpp/lfalloc/dbg_info
        library/cpp/ytalloc/api
    )
ENDIF()

IF(ALLOCATOR == "TCMALLOC_256K")
    SRCS(tcmalloc.cpp)
    PEERDIR(contrib/libs/tcmalloc)
ELSE()
    SRCS(tcmalloc_null.cpp)
ENDIF()

END() 
