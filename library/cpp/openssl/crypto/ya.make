LIBRARY()

PEERDIR(
    contrib/libs/openssl
    library/cpp/openssl/big_integer
    library/cpp/openssl/init
)

SRCS(
    sha.cpp
    rsa.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
