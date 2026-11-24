ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_TEST_DISABLE_MLOCK=1)

SUPPRESSIONS(
    ${ARCADIA_ROOT}/ydb/tests/tsan.supp
)

DEPENDS(
    ydb/apps/ydbd
    ydb/public/tools/ydb_recipe
)

USE_RECIPE(
    ydb/public/tools/ydb_recipe/ydb_recipe --suppress-version-check
)
