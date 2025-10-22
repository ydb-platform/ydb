PY3TEST()

TEST_SRCS(
    test_ttl.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
    contrib/python/PyHamcrest
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
