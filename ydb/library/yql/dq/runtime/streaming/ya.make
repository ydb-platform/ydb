LIBRARY()

SRCS(
    dq_compute_actor_watermarks.cpp
    dq_watermark_generator_tracker.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
