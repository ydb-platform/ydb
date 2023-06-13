LIBRARY()

SRCS(
    bit.h
    compressor.h
    huff.h
    metainfo.h
    lib.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
