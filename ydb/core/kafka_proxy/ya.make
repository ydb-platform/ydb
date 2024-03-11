LIBRARY()

SRCS(
    actors/kafka_api_versions_actor.cpp
    actors/kafka_init_producer_id_actor.cpp
    actors/kafka_metadata_actor.cpp
    actors/kafka_produce_actor.cpp
    actors/kafka_sasl_auth_actor.cpp
    actors/kafka_sasl_handshake_actor.cpp
    actors/kafka_metrics_actor.cpp
    actors/kafka_list_offsets_actor.cpp
    actors/kafka_topic_offsets_actor.cpp
    actors/kafka_fetch_actor.cpp
    actors/kafka_find_coordinator_actor.cpp
    actors/kafka_read_session_actor.cpp
    actors/kafka_offset_fetch_actor.cpp
    actors/kafka_offset_commit_actor.cpp
    actors/kafka_create_topics_actor.cpp
    actors/kafka_create_partitions_actor.cpp
    actors/kafka_alter_configs_actor.cpp
    kafka_connection.cpp
    kafka_connection.h
    kafka_constants.h
    kafka_listener.h
    kafka.h
    kafka_log.h
    kafka_log_impl.h
    kafka_messages.cpp
    kafka_messages.h
    kafka_messages_int.cpp
    kafka_messages_int.h
    kafka_proxy.h
    kafka_records.cpp
    kafka_consumer_protocol.cpp
    kafka_metrics.cpp
)

GENERATE_ENUM_SERIALIZATION(kafka.h)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/core/base
    ydb/core/protos
    ydb/core/raw_socket
    ydb/services/persqueue_v1
)

END()

RECURSE_FOR_TESTS(
    ut
)
