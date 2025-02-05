UNITTEST()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

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
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/table
    ydb/public/lib/ydb_cli/commands/topic_workload
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

END()
