PY3TEST()

TEST_SRCS(test.py)

IF (SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
REQUIREMENTS(
    ram:32
    cpu:4
)

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
