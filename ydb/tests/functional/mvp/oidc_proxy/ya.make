PY3TEST()

FORK_TEST_FILES()
SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    oidc_proxy_env.py
    test_oidc_proxy.py
)

DEPENDS(
    ydb/mvp/oidc_proxy/bin
)

PEERDIR(
    contrib/python/requests
    ydb/tests/library
)

END()
