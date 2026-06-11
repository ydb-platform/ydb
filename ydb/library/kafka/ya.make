LIBRARY()

SRCS(
    kafka.h
    kafka_log.h
    kafka_log_impl.h
    kafka_messages_int.cpp
    kafka_messages_int.h
    kafka_records.cpp
    kafka_records.h
    kafka_write_buffer.cpp
    kafka_write_buffer.h
)

GENERATE_ENUM_SERIALIZATION(kafka.h)

PEERDIR(
    ydb/core/protos
    library/cpp/streams/zstd
    ydb/library/actors/core
    ydb/library/services
    yql/essentials/public/decimal
)

END()

RECURSE_FOR_TESTS(
    ut
)

