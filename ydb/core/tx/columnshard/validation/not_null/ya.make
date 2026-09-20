LIBRARY()

SRCS(
    validator.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
)

END()

RECURSE_FOR_TESTS(
    ut
)
