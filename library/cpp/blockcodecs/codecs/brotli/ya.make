LIBRARY()

PEERDIR(
    contrib/libs/brotli/c/enc
    contrib/libs/brotli/c/dec
    library/cpp/blockcodecs/core
)

SRCS(
    GLOBAL brotli.cpp
)

END()
