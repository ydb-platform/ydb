PY3TEST()

TEST_SRCS(
    test_whoami.py
)

PEERDIR(
    contrib/python/requests
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/security/oidc/recipe.inc)

END()
