LIBRARY()

PEERDIR(
    library/cpp/containers/absl_flat_hash
    library/cpp/blockcodecs/core
    library/cpp/blockcodecs/codecs/zstd
)

SRCS(
    registry.cpp
    resource.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
