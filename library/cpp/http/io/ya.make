LIBRARY()

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/streams/brotli
    library/cpp/streams/bzip2
    library/cpp/streams/lzma
)

SRCS(
    chunk.cpp
    compression.cpp
    headers.cpp
    stream.cpp
)

END()

RECURSE(
    list_codings
)

RECURSE_FOR_TESTS(
    benchmark
    fuzz
    ut
)
