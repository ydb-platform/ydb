LIBRARY()

SRCS(
    spack_v1_decoder.cpp
    spack_v1_encoder.cpp
    varint.cpp
    compression.cpp
)

PEERDIR(
    library/cpp/monlib/encode/buffered
    library/cpp/monlib/exception

    contrib/libs/lz4
    contrib/libs/xxhash
    contrib/libs/zlib
    contrib/libs/zstd
)

END()

RECURSE(
    fuzz
    ut
)
