LIBRARY()

SRCS(
    rsa_key_pair.cpp
)

PEERDIR(
    library/cpp/string_utils/base64
    contrib/libs/openssl
)

END()
