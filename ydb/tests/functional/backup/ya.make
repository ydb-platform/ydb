UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

PEERDIR(
    library/cpp/threading/local_executor
    library/cpp/yson
    ydb/library/testlib/s3_recipe_helper
    ydb/public/sdk/cpp/client/ydb_export
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/draft
    ydb/public/lib/yson_value
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

REQUIREMENTS(ram:16)

END()
