LIBRARY()

SRCS(
    dread_cache_service/caching_service.cpp
    pq.cpp
)

PEERDIR(
    ydb/core/persqueue/public/cluster_tracker
    ydb/core/persqueue/public/fetcher
    ydb/core/persqueue/public/list_topics
    ydb/core/persqueue/partition_key_range
    ydb/core/persqueue/pqrb
    ydb/core/persqueue/pqtablet
    ydb/core/persqueue/writer
)

END()

RECURSE(
    common
    config
    events
    partition_index_generator
    partition_key_range
    pqrb
    pqtablet
    public
    writer
)

RECURSE_FOR_TESTS(
    ut
    dread_cache_service/ut
    ut/slow
    ut/ut_with_sdk
)
