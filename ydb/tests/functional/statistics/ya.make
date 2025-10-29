PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

PY_SRCS (
    conftest.py
)

TEST_SRCS(
    test_restarts.py
)

SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
