LIBRARY()

PEERDIR(
    contrib/libs/brotli/c/enc
    contrib/libs/brotli/c/dec
)

SRCS(
    brotli.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
