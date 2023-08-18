UNITTEST()

IF (SANDBOXING OR SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(ya:fat)
ENDIF()

DEPENDS(
    ydb/apps/ydb
)

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

SRCS(
    workload-topic.cpp
    workload-transfer-topic-to-table.cpp
    run_ydb.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_table
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

END()
