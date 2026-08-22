LIBRARY()

SRCS(
    actor.cpp
    common_app.cpp
    heartbeat.cpp
    key.cpp
    microseconds_sliding_window.cpp
    partition_id.cpp
    percentiles.cpp
    partitioning_keys_manager.cpp
)

GENERATE_ENUM_SERIALIZATION(sourceid_info.h)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/persqueue/public
    ydb/core/persqueue/public/partition_key_range
    ydb/library/actors/core
    ydb/library/logger
    ydb/library/kll_median
)

END()

RECURSE(
    proxy
)

RECURSE_FOR_TESTS(
    ut
)
