LIBRARY()

SRCS(
    tz.cpp
)

PEERDIR(
)

END()

RECURSE(
    gen
)

RECURSE_FOR_TESTS(
    ut
)
