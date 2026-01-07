LIBRARY()

PEERDIR(
    contrib/libs/openssl
    library/cpp/unicode/normalization
)

SRCS(
    saslprep.cpp
    scram.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
