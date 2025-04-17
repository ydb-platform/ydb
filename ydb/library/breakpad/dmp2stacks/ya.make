IF (OS_LINUX)

PROGRAM()

    PEERDIR (
        contrib/libs/breakpad/src
        contrib/libs/breakpad/src/client/linux
        contrib/libs/llvm16/lib/DebugInfo/Symbolize
        library/cpp/getopt
    )

    SRCS (
        main.cpp
    )

END()

ENDIF()
