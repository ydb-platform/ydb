LIBRARY()

SRCS(
    cluster_tracker.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/grpc_services/cancelation
)

END()
