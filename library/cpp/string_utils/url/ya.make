LIBRARY()

SRCS(
    url.cpp
    url.h
)

END()

RECURSE(
    fuzz
)

RECURSE_FOR_TESTS(
    ut
)
