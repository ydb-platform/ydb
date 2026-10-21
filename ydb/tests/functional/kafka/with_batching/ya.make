UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_FEATURE_FLAGS="enable_kafka_native_balancing,enable_kafka_transactions,enable_topic_messages_batching,enable_topic_write_offset_delta_in_keys")

PEERDIR(
    ydb/tests/functional/kafka/test_common
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:2)
ENDIF()

END()
