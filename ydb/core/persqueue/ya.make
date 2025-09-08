LIBRARY()

SRCS(
    actor_persqueue_client_iface.h
    cluster_tracker.cpp
    heartbeat.cpp
    key.cpp
    list_all_topics_actor.cpp
    percentile_counter.cpp
    pq_l2_cache.cpp
    type_codecs_defs.cpp
    write_meta.cpp
    dread_cache_service/caching_service.cpp
    write_id.cpp
)

GENERATE_ENUM_SERIALIZATION(sourceid_info.h)

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
    ydb/core/persqueue/codecs
    ydb/core/persqueue/config
    ydb/core/persqueue/events
    ydb/core/persqueue/partition_key_range
    ydb/core/persqueue/pqrb
    ydb/core/persqueue/pqtablet
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/library/logger
    ydb/library/persqueue/counter_time_keeper
    ydb/library/persqueue/topic_parser
    ydb/library/protobuf_printer
    ydb/public/lib/base
    ydb/public/sdk/cpp/src/client/persqueue_public
    #ydb/library/dbgtrace
)

END()

RECURSE(
    codecs
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
