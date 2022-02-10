LIBRARY()

OWNER(
    spuchin
    g:kikimr
)

SRCS(
    dq_compute_actor.cpp
    dq_async_compute_actor.cpp
    dq_compute_actor_channels.cpp
    dq_compute_actor_checkpoints.cpp
    dq_compute_actor_io_actors_factory.cpp
    dq_compute_actor_stats.cpp
    dq_compute_issues_buffer.cpp 
    retry_queue.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/runtime
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet_flat
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/minikql/computation
)

YQL_LAST_ABI_VERSION() 

END() 

RECURSE_FOR_TESTS( 
    ut 
) 
