UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_FEATURE_FLAGS="enable_kafka_native_balancing,enable_kafka_transactions,enable_topic_messages_batching,enable_topic_write_offset_delta_in_keys")

SRCDIR(ydb/tests/functional/kafka)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/kafka/tests.inc)

END()
