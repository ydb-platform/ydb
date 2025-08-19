PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

BUNDLE(
    ydb/apps/ydb NAME ydb_cli
)

RESOURCE(
    ydb_cli ydb_cli
    ydb/tests/stress/kafka/resources/e2e-kafka-api-tests-1.0-SNAPSHOT-all.jar e2e-kafka-api-tests-1.0-SNAPSHOT-all.jar
)

PEERDIR(
    library/python/monlib
    library/python/resource
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
