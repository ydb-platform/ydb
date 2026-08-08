LIBRARY()

SRCS(
    config.cpp
    inflight_limiter.cpp
    pq_database.cpp
    pq_rl_helpers.cpp
    utils.cpp
    write_id.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/persqueue/public/cloud_events
)

END()

RECURSE(
    cluster_tracker
    codecs
    counters
    describer
    fetcher
    list_topics
    mlp
    partition_index_generator
    partition_key_range
    write_meta
    cloud_events
)
