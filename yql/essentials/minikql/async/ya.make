LIBRARY()

SRCS(
    async.cpp
    resources.cpp
    scheduler.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    compute
)
