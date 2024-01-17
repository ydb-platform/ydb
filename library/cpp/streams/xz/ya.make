LIBRARY()

PEERDIR(
    contrib/libs/lzma
)

SRCS(
    decompress.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
