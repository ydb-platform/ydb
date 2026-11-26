PY3TEST()

STYLE_PYTHON()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    ydb/tests/tools/datastreams_helpers
    ydb/tests/fq/streaming_common
    ydb/tests/library
    ydb/public/api/protos
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/pytest
)

DEPENDS(
    ydb/apps/ydb
)

PY_SRCS(
    conftest.py
    test_base.py
)

TEST_SRCS(
    test_alloc_default.py
    test_dc_local.py
    test_result_limits.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
