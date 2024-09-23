LIBRARY()

IF (OS_LINUX)
    PEERDIR(
        contrib/restricted/abseil-cpp/absl/debugging
    )
    SRCS(GLOBAL fix.cpp)
ENDIF()

END()
