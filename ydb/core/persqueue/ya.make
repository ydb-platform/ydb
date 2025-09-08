LIBRARY()

SRCS(
    dread_cache_service/caching_service.cpp
)

PEERDIR(
    contrib/libs/fmt
    ydb/library/actors/core
    library/cpp/html/pcdata
    library/cpp/json
    ydb/core/backup/impl
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/keyvalue
    ydb/core/kqp/common
    ydb/core/metering
    ydb/core/persqueue/config
    ydb/core/persqueue/events
    ydb/core/protos
    ydb/library/logger
    ydb/library/persqueue/counter_time_keeper
    ydb/library/persqueue/topic_parser
    ydb/library/protobuf_printer
    ydb/public/lib/base
    ydb/public/sdk/cpp/src/client/persqueue_public
    #ydb/library/dbgtrace

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
