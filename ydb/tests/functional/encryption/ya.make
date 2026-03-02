PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
TEST_SRCS(
    test_encryption.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

END()
