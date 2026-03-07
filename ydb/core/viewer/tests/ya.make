PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(test.py)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()
DEPENDS(
)

PEERDIR(
    contrib/python/requests
    contrib/python/urllib3
    ydb/tests/library
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
