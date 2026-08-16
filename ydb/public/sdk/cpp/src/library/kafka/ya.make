LIBRARY()

SRCS(
    kafka.h
    kafka_log.h
    kafka_messages_int.cpp
    kafka_messages_int.h
    kafka_records.cpp
    kafka_records.h
    kafka_write_buffer.cpp
    kafka_write_buffer.h
)

GENERATE_ENUM_SERIALIZATION(kafka.h)

PEERDIR(
    library/cpp/streams/zstd
    ydb/public/sdk/cpp/src/library/decimal
)

END()

RECURSE_FOR_TESTS(
    ut
)
