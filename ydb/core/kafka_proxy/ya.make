LIBRARY()

SRCS(
    kafka_connection.cpp
    kafka_connection.h
    kafka_listener.h
    kafka.h
    kafka_log.h
    kafka_log_impl.h
    kafka_messages.cpp
    kafka_messages.h
    kafka_messages_int.cpp
    kafka_messages_int.h
    kafka_produce_actor.cpp
    kafka_produce_actor.h
    kafka_proxy.h
    kafka_records.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    ydb/core/base
    ydb/core/protos
    ydb/core/raw_socket
)

END()

RECURSE_FOR_TESTS(
    ut
)
