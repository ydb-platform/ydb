LIBRARY()

SRCS(
    async.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    compute
)
