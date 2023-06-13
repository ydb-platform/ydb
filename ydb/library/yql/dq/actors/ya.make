LIBRARY()

GRPC()

SRCS(
    dq.cpp
    dq_events_ids.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/protos
)

END()

RECURSE(
    compute
    protos
    task_runner
)
