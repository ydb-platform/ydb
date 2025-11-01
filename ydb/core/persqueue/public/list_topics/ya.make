LIBRARY()

SRCS(
    list_all_topics_actor.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/grpc_services/cancelation
    ydb/core/tx/scheme_cache
)

END()

RECURSE_FOR_TESTS(
    ut
)
