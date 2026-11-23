LIBRARY()

SRCS(
    intervals.cpp
)

PEERDIR(
    ydb/core/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
