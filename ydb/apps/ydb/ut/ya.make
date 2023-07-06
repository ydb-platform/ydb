UNITTEST()

DEPENDS(
    ydb/apps/ydb
)

SRCS(
    workload-topic.cpp
    workload-transfer-topic-to-table.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_table
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

REQUIREMENTS(ram:16)

END()
