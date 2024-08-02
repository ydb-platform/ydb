LIBRARY()

NO_UTIL()
ALLOCATOR_IMPL()


IF (OS_ANDROID)
    PEERDIR(
        library/cpp/malloc/system
    )
ELSE()
    PEERDIR(
        library/cpp/malloc/api
        contrib/libs/jemalloc
    )
    SRCS(
        malloc-info.cpp
    )
ENDIF()

END()
