LIBRARY()

OWNER(pg)

PEERDIR(
    certs
    contrib/libs/openssl
    library/cpp/openssl/init
    library/cpp/openssl/method
)

SRCS(
    stream.cpp
)

END()
