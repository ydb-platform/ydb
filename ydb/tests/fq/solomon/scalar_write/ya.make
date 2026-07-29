PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_scalar_solomon_write.py
)

PY_SRCS(
    conftest.py
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2 ram:16)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/library/yql/tools/solomon_emulator/client
    ydb/tests/fq/streaming_common
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/tests/tools/datastreams_helpers
    ydb/tests/tools/fq_runner
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
