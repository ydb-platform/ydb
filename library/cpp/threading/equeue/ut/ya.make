UNITTEST()

PEERDIR(
    ADDINCL library/cpp/threading/equeue
    library/cpp/threading/equeue/fast
)

SRCDIR(library/cpp/threading/equeue)

SRCS(
    equeue_ut.cpp
)

END()
