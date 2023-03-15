LIBRARY()

SRCS(
    hyperloglog.h
    hyperloglog.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
