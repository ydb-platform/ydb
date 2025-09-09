LIBRARY()

SRCS(
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
    fetcher
    list_topics
    write_meta
)
