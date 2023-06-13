LIBRARY()

SRCS(
    hmac.cpp
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/string_utils/base64
)

END()

RECURSE_FOR_TESTS(
    ut
)
