LIBRARY()

SRCS(
    http_parser.cpp
)

PEERDIR(
    library/cpp/http/io
    library/cpp/blockcodecs/core
    library/cpp/blockcodecs/codecs/brotli
    library/cpp/blockcodecs/codecs/bzip
    library/cpp/blockcodecs/codecs/fastlz
    library/cpp/blockcodecs/codecs/lz4
    library/cpp/blockcodecs/codecs/lzma
    library/cpp/blockcodecs/codecs/snappy
    library/cpp/blockcodecs/codecs/zlib
    library/cpp/blockcodecs/codecs/zstd
    library/cpp/streams/brotli
)

END()

RECURSE_FOR_TESTS(ut)
