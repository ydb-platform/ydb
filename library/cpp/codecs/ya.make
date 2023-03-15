LIBRARY()

SRCS(
    tls_cache.cpp
    codecs.cpp
    codecs_registry.cpp
    comptable_codec.cpp
    delta_codec.cpp
    float_huffman.cpp
    huffman_codec.cpp
    pfor_codec.cpp
    solar_codec.cpp
    zstd_dict_codec.cpp
)

PEERDIR(
    contrib/libs/zstd
    library/cpp/bit_io
    library/cpp/blockcodecs
    library/cpp/codecs/greedy_dict
    library/cpp/comptable
    library/cpp/containers/comptrie
    library/cpp/deprecated/accessors
    library/cpp/packers
    library/cpp/string_utils/relaxed_escaper
)

END()

RECURSE(
    greedy_dict
    float_huffman_bench
    ut
)
