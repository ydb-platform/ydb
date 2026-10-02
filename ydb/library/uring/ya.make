LIBRARY()

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/liburing
    )
    SRCS(
        liburing_linux.h
    )
ENDIF(OS_LINUX)

SRCS(
    liburing_compat.h
)

END()
