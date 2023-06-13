LIBRARY()

NO_UTIL()
ALLOCATOR_IMPL()
NO_COMPILER_WARNINGS()

IF (OS_WINDOWS)
    PEERDIR(
        library/cpp/lfalloc
    )
ELSE()
    SRCS(
        balloc.cpp
        malloc-info.cpp
    )

    PEERDIR(
        library/cpp/balloc/lib
    )
ENDIF()

END()

NEED_CHECK()
