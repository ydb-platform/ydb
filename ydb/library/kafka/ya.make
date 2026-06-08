LIBRARY()

SRCS(
    kafka.h
    kafka_log.h
    kafka_log_impl.h
    kafka_messages_int.cpp
    kafka_messages_int.h
    kafka_records.cpp
    kafka_records.h
)

GENERATE_ENUM_SERIALIZATION(kafka.h)

PEERDIR(
    ydb/core/protos
    ydb/core/raw_socket
)

END()
