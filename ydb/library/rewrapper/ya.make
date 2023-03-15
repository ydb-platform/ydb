LIBRARY()

PEERDIR(
    ydb/library/rewrapper/proto
)

SRCS(
    dispatcher.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
