UNITTEST()

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydb
)

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_FEATURE_FLAGS="enable_topic_service_tx")

SRCS(
    workload-topic.cpp
    workload-transfer-topic-to-table.cpp
    run_ydb.cpp
    supported_codecs.cpp
    supported_codecs_fixture.cpp
    ydb-dump.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_table
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

END()
