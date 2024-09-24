IF (NOT SANITIZER_TYPE)

PY3TEST()

TEST_SRCS(test.py)

TIMEOUT(600)
SIZE(MEDIUM)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_FEATURE_FLAGS="enable_resource_pools")

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
    contrib/python/PyHamcrest
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

FORK_SUBTESTS()
FORK_TEST_FILES()
END()

ENDIF()
