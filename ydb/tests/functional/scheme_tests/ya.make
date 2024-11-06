PY3TEST()
TEST_SRCS(
    tablet_scheme_tests.py
)

TIMEOUT(600)
SIZE(MEDIUM)

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
