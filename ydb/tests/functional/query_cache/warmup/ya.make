PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
TEST_SRCS(
    test_warmup.py
)

SIZE(LARGE)
TAG(ya:fat)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ENDIF()

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

END()
