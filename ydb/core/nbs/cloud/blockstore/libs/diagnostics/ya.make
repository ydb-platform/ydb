LIBRARY()

SRCS(
    dbg_counters.cpp
    downtime_history.cpp
    probes.cpp
    public.cpp
    trace_helpers.cpp
    user_counter.cpp
    vchunk_counters.cpp
    vhost_stats_simple.cpp
    vhost_stats_test.cpp
    vhost_stats.cpp
    volume_counters.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/storage/core/libs/diagnostics
    ydb/core/nbs/cloud/storage/core/protos
    ydb/core/nbs/cloud/storage/core/libs/user_stats/counter

    ydb/library/actors/wilson

    library/cpp/containers/ring_buffer
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    util
)

END()

RECURSE_FOR_TESTS(
    gtest
    ut
)
