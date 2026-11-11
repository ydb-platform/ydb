PY3TEST()

FORK_SUBTESTS()
# Keep test cases isolated: audit log capture can affect neighbouring tests.
SPLIT_FACTOR(40)
SIZE(MEDIUM)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_DSTOOL_BINARY="ydb/apps/dstool/ydb-dstool")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    helpers.py
    http_helpers.py
    test_auditlog.py
    test_canonical_records.py
    test_canonical_records_legacy_dstool.py
)

DEPENDS(
    ydb/apps/dstool
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:10 cpu:1)
ENDIF()

END()
