LIBRARY()

SRCS(
    prometheus_decoder.cpp
    prometheus_encoder.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/monlib/encode/buffered
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(prometheus.h)

END()

RECURSE(
    fuzz
    ut
)
