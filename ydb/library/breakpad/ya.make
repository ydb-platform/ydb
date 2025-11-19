LIBRARY()

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/breakpad/src
        contrib/libs/breakpad/src/client/linux
    )

    SRCS(
        GLOBAL minidumps.cpp
    )

    RESOURCE(about.txt "internal_breakpad_about")

ENDIF()

END()
