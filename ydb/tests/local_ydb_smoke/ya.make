PY3TEST()

SIZE(MEDIUM)
TAG(ya:not_autocheck)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
DEPENDS(
    ydb/public/tools/local_ydb
)

PEERDIR(
    library/python/port_manager
)

TEST_SRCS(
    test_local_ydb_smoke.py
)

END()
