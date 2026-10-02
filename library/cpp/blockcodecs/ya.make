LIBRARY()

PEERDIR(
    library/cpp/blockcodecs/core
    library/cpp/blockcodecs/codecs/brotli
    library/cpp/blockcodecs/codecs/bzip
    library/cpp/blockcodecs/codecs/fastlz
    library/cpp/blockcodecs/codecs/lz4
    library/cpp/blockcodecs/codecs/lzma
    library/cpp/blockcodecs/codecs/snappy
    library/cpp/blockcodecs/codecs/zlib
    library/cpp/blockcodecs/codecs/zstd
)

IF (NOT DONT_LINK_LEGACY_ZSTD06_BLOCKCODEC)
PEERDIR(
    library/cpp/blockcodecs/codecs/legacy_zstd06
)
ENDIF()

SRCS(
    codecs.cpp
    stream.cpp
)

END()

RECURSE_FOR_TESTS(
    fuzz
    ut
)
