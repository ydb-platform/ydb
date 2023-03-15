LIBRARY()

PEERDIR(
    library/cpp/threading/future
    library/cpp/deprecated/atomic
)

SRCS(
    scheduler.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
