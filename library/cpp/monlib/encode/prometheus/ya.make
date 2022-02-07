LIBRARY()

OWNER(
    jamel
    g:solomon
)

SRCS(
    prometheus_decoder.cpp
    prometheus_encoder.cpp
)

PEERDIR(
    library/cpp/monlib/encode
)

END()
