PY3TEST()

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    contrib/python/requests
    contrib/python/httpx
)

TEST_SRCS(
    test_http.py
)

DEPENDS(
    yt/yt/library/ytprof/example
)

END()
