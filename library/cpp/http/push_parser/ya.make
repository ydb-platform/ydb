LIBRARY()

SRCS(
    http_parser.cpp
)

PEERDIR(
    library/cpp/http/io
    library/cpp/blockcodecs
    library/cpp/streams/brotli
)

END()

RECURSE_FOR_TESTS(ut)
