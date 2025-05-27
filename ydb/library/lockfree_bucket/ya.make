LIBRARY()


SRCS(
    lockfree_bucket.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
