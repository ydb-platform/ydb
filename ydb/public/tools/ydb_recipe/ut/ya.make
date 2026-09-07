PY3TEST()

ENV(YDB_ENABLE_SQS_TOPIC_API=true)
ENV(YDB_FEATURE_FLAGS="enable_topic_message_level_parallelism")

TEST_SRCS(
    test_sqs_topic_api.py
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/oss/ydb_sdk_import
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)
TIMEOUT(120)

END()
