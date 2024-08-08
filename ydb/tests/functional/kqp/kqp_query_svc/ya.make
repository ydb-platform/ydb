UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/public/lib/ut_helpers
    ydb/public/sdk/cpp/client/ydb_discovery
    ydb/public/sdk/cpp/client/ydb_query
    ydb/public/sdk/cpp/client/draft
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
TIMEOUT(30)

REQUIREMENTS(ram:16)

END()
