UNITTEST()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/draft
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

END()
