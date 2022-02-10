UNITTEST()

OWNER(
    g:base
    g:middle
)

PEERDIR(
    ADDINCL library/cpp/threading/equeue
)

SRCDIR(library/cpp/threading/equeue)

SRCS(
    equeue_ut.cpp
)

END()
