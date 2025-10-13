PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    yql/essentials/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
)

END()
