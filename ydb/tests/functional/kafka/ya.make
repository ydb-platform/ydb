UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_FEATURE_FLAGS="enable_kafka_native_balancing,enable_kafka_transactions")

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/kafka/tests.inc)

END()

RECURSE_FOR_TESTS(
    with_batching
)
