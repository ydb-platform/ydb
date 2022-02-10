LIBRARY()

OWNER(
    alexnick
    g:kikimr 
    g:logbroker
)

SRCS(
    cluster_tracker.cpp
    blob.cpp
    event_helpers.cpp
    header.cpp
    percentile_counter.cpp
    pq.cpp
    pq_database.cpp
    pq_impl.cpp
    sourceid.cpp
    mirrorer.cpp
    mirrorer.h
    ownerinfo.cpp
    partition.cpp
    pq_l2_cache.cpp
    read_balancer.cpp
    read_speed_limiter.cpp
    subscriber.cpp
    type_codecs_defs.cpp
    user_info.cpp
    write_meta.cpp
    actor_persqueue_client_iface.h
)

GENERATE_ENUM_SERIALIZATION(sourceid.h)

PEERDIR(
    library/cpp/actors/core 
    library/cpp/html/pcdata 
    library/cpp/json 
    ydb/core/base 
    ydb/core/engine/minikql 
    ydb/core/keyvalue 
    ydb/core/metering 
    ydb/core/persqueue/codecs 
    ydb/core/persqueue/config 
    ydb/core/persqueue/events 
    ydb/core/persqueue/partition_key_range 
    ydb/core/persqueue/writer 
    ydb/core/protos 
    ydb/library/persqueue/counter_time_keeper 
    ydb/library/persqueue/topic_parser 
    ydb/public/lib/base 
    ydb/public/sdk/cpp/client/ydb_persqueue_core 
)

END()
 
RECURSE_FOR_TESTS( 
    ut 
    ut_slow 
) 
