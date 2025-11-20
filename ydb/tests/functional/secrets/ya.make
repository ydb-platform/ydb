PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    conftest.py
    test_secrets.py
    test_secrets_usage.py
)

SPLIT_FACTOR(20)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/flavours/flavours_deps.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

DEPENDS(
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/library/flavours
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
