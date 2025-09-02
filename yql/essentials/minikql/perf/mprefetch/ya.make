PROGRAM()

PEERDIR(
	library/cpp/getopt
)

SRCS(
    mprefetch.cpp
)

IF (ARCH_X86_64)

CFLAGS(
    -mprfchw
)

ENDIF()

YQL_LAST_ABI_VERSION()

END()
