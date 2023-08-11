LIBRARY()

IF (OS_IOS OR OS_IOSSIM)
    CFLAGS(-O3)
ENDIF()

PEERDIR(
    contrib/libs/lzmasdk
    library/cpp/blockcodecs/core
)

SRCS(
    GLOBAL lzma.cpp
)

IF (OS_WINDOWS)
    IF (NOT MSVC)
        CFLAGS(
            -Wno-unused-command-line-argument
        )
    ENDIF()
ENDIF()

END()
