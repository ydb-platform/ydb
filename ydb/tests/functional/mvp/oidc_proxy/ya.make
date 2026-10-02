PY3TEST()

FORK_TEST_FILES()
SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

ENV(OIDC_PROXY_BINARY="ydb/mvp/oidc_proxy/bin/mvp_oidc_proxy")

TEST_SRCS(
    conftest.py
    oidc_proxy_env.py
    oidc_proxy_testlib.py
    test_auth_failures.py
    test_basic_scenarios.py
    test_streaming.py
)

DEPENDS(
    ydb/mvp/oidc_proxy/bin
)

PEERDIR(
    contrib/python/requests
    ydb/tests/functional/mvp/common
    ydb/tests/library
)

END()
