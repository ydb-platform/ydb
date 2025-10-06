LIBRARY()

SRCS(
    describer.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
)

END()

RECURSE_FOR_TESTS(
#    ut
)
