PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/federation_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner.inc)

TEST_SRCS(
    test_logbroker.py
)

PY_SRCS(
    conftest.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)
REQUIREMENTS(ram:12)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:20)
ENDIF()

PEERDIR(
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/fq/streaming_common
    ydb/tests/tools/datastreams_helpers
)

DEPENDS(
    ydb/apps/ydb
)

END()
