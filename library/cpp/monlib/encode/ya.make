LIBRARY()

SRCS(
    encoder.cpp
    encoder_state.cpp
    format.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
)

GENERATE_ENUM_SERIALIZATION_WITH_HEADER(encoder_state_enum.h)

END()

RECURSE(
    fuzz
    ut
)
