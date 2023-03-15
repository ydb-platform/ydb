LIBRARY()

SRCS(
    ipmath.cpp
    range_set.cpp
)

PEERDIR(library/cpp/ipv6_address)

END()

RECURSE_FOR_TESTS(ut)
