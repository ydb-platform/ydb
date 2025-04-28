UNITTEST()

ENV(S3_IGNORE_SUBDOMAIN_BUCKETNAME=true)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

PEERDIR(
    ydb/tests/functional/backup/helpers
)

SRCS(
    s3_path_style_backup_ut.cpp
)

ENV(YDB_FEATURE_FLAGS="enable_encrypted_export")

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

END()
