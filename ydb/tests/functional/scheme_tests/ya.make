PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
TEST_SRCS(
    tablet_scheme_tests.py
)

SIZE(MEDIUM)

DEPENDS(
)

DATA(
    arcadia/ydb/tests/functional/scheme_tests/canondata
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/canonical
)

END()
