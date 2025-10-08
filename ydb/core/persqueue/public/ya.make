LIBRARY()

SRCS(
    config.cpp
    pq_database.cpp
    pq_rl_helpers.cpp
    utils.cpp
    write_id.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
)

END()

RECURSE(
    cluster_tracker
    codecs
    counters
    describer
    fetcher
    list_topics
    partition_index_generator
    partition_key_range
    write_meta
)
