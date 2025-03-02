LIBRARY()

SRCS(
    actor_distributor_service.cpp
    actor_worker.cpp
    task_distributor_service.cpp
    task_worker.cpp
)

PEERDIR(
    ydb/core/tx/conveyor/usage
    ydb/core/protos
)

END()
