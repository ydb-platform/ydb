LIBRARY()

SRCS(
    prometheus_decoder.cpp
    prometheus_encoder.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/monlib/encode/buffered
)

END()

RECURSE(
    fuzz
    ut
)
