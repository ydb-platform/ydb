LIBRARY()

SRCS(
    json_decoder.cpp
    json_encoder.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/monlib/encode/buffered
    library/cpp/monlib/exception
    library/cpp/json
    library/cpp/json/writer
)

END()

RECURSE_FOR_TESTS(
    ut
)
