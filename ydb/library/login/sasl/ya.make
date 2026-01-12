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
