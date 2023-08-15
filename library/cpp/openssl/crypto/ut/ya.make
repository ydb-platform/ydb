UNITTEST_FOR(library/cpp/openssl/crypto)

PEERDIR(
    contrib/libs/openssl
)

SRCS(
    rsa_ut.cpp
    sha_ut.cpp
)

END()
