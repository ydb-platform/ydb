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
    common
    compute
    input_transforms
    spilling
    task_runner
)
