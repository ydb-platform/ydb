PROGRAM()

PEERDIR(
	library/cpp/getopt
)

SRCS(
    mprefetch.cpp
)

CFLAGS(
    -mprfchw
)

YQL_LAST_ABI_VERSION()

END()
