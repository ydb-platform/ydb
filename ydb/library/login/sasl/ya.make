LIBRARY()

PEERDIR(
    contrib/libs/openssl
    library/cpp/unicode/normalization
)

SRCS(
    plain.cpp
    saslprep.cpp
    scram.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
