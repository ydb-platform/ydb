PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    contrib/python/requests
)

TEST_SRCS(
    test_solomon.py
)

DEPENDS(
    yt/yt/library/profiling/example
)

END()
