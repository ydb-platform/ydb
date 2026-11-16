LIBRARY()

PEERDIR(
    library/cpp/containers/absl
    library/cpp/blockcodecs/core
    library/cpp/blockcodecs/codecs/zstd
)

SRCS(
    registry.cpp
    resource.cpp
    static_blob_default.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
