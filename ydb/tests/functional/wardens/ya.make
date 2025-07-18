PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_liveness_wardens.py
)

DEPENDS(
)

PEERDIR(
    ydb/public/sdk/python
    ydb/tests/library
    ydb/tests/library/clients
)

SIZE(MEDIUM)

END()
