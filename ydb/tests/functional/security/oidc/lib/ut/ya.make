PY3TEST()

TEST_SRCS(
    test_keycloak_client.py
)

PEERDIR(
    ydb/tests/functional/security/oidc/lib
)

END()
