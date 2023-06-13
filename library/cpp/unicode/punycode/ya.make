LIBRARY()

PEERDIR(
    contrib/libs/libidn
)

SRCS(
    punycode.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
