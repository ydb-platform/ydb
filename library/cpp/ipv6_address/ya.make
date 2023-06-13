LIBRARY()

SRCS(
    ipv6_address.cpp
    ipv6_address.h
    ipv6_address_p.h
)

PEERDIR(
    library/cpp/int128
)

END()

RECURSE_FOR_TESTS(
    ut
)
