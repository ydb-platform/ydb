LIBRARY()

NO_UTIL()
NO_COMPILER_WARNINGS()

SRCS(
    alloc_stats.cpp
    alloc_stats.h
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/linuxvdso
    )
ENDIF()

PEERDIR(
    library/cpp/balloc/setup
    library/cpp/malloc/api
)

SET(IDE_FOLDER "util")

END()

NEED_CHECK()
