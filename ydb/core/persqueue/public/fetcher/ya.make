LIBRARY()

SRCS(
    fetch_request_actor.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
)

END()

RECURSE_FOR_TESTS(
    ut
)
