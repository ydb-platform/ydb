PY3TEST()

STYLE_PYTHON()

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/federation_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner.inc)

TEST_SRCS(
    test_logbroker.py
)

PY_SRCS(
    conftest.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
REQUIREMENTS(ram:12 cpu:1)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:20 cpu:1)
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
