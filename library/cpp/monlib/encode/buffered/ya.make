LIBRARY()

SRCS(
    buffered_encoder_base.cpp
    string_pool.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/monlib/metrics
)

END()

RECURSE_FOR_TESTS(
    ut
)
