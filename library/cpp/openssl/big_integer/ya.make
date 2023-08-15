LIBRARY()

PEERDIR(
    contrib/libs/openssl
)

SRCS(
    big_integer.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
