UNITTEST()

PEERDIR(
    library/cpp/string_utils/base64
    library/cpp/codecs
    library/cpp/string_utils/relaxed_escaper
)

SRCS(
    tls_cache_ut.cpp
    codecs_ut.cpp
    float_huffman_ut.cpp
)

END()
