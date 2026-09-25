LIBRARY()

SRCS(
    activeactors.cpp
    activeactors.h
    actor_liveness_checker.cpp
    actor_liveness_checker.h
    collector_counters.cpp
    future_callback.h
    mon_histogram_helper.h
    pool_stats_collector.cpp
    selfping_actor.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
)

END()

RECURSE_FOR_TESTS(
    ut
)
