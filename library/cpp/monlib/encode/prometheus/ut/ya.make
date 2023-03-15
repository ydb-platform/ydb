UNITTEST_FOR(library/cpp/monlib/encode/prometheus)

SRCS(
    prometheus_encoder_ut.cpp
    prometheus_decoder_ut.cpp
)

PEERDIR(
    library/cpp/monlib/encode/protobuf
)

END()
