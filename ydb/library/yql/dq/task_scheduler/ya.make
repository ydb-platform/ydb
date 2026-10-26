LIBRARY()

SRCS(
    task_classification.cpp
    task_trace.cpp
    task_graph.cpp
    task_scheduler.cpp
    worker_pool.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/monlib/dynamic_counters
    library/cpp/time_provider
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    compare
    ut
)
