PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TEST_SRCS(
    test_dynumber.py
)

PEERDIR(
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

END()
