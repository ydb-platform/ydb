LIBRARY()

GRPC()

SRCS(
    dq.cpp
    dq_events_ids.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/protos
    yql/essentials/public/issue
)

END()

RECURSE(
    common
    compute
    input_transforms
    spilling
    task_runner
)
