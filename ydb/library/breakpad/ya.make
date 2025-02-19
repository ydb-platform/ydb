LIBRARY()

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/breakpad/src
        contrib/libs/breakpad/src/client/linux
    )

    SRCS(
        GLOBAL minidumps.cpp
    )
ENDIF()

END()
