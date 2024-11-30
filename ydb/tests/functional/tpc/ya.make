IF (NOT SANITIZER_TYPE)

PY3TEST()

PY_SRCS(
    conftest.py
)

TEST_SRCS(
    test_tpch.py
)

TIMEOUT(600)
SIZE(MEDIUM)

REQUIREMENTS(ram:16)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_FEATURE_FLAGS="enable_resource_pools")

PEERDIR(
    ydb/tests/olap/load/lib
)

DEPENDS(
    ydb/apps/ydb
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

FORK_TEST_FILES()
END()

ENDIF()
