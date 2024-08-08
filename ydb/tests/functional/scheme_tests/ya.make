PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TEST_SRCS(
    tablet_scheme_tests.py
)

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

DEPENDS(
    ydb/apps/ydbd
)

DATA(
    arcadia/ydb/tests/functional/scheme_tests/canondata
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/canonical
)

END()
