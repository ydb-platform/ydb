UNITTEST()

ENV(S3_IGNORE_SUBDOMAIN_BUCKETNAME=true)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

PEERDIR(
    ydb/library/testlib/s3_recipe_helper
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/draft
)

SRCS(
    s3_path_style_backup_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

END()
