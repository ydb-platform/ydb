LIBRARY()

SRCS(
    equeue.h
    equeue.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
