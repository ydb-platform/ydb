LIBRARY()

SRCS(
    interconnect_address.cpp
    interconnect_address.h
)

PEERDIR(
    contrib/libs/libc_compat
)

END()

RECURSE_FOR_TESTS(
    ut
)
