LIBRARY()

NO_UTIL()

OWNER(nga)

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
