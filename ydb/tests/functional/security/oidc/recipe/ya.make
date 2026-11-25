PY3_PROGRAM(oidc_recipe)

PY_SRCS(
    __main__.py
    keycloak.py
    tls.py
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/cryptography
    contrib/python/grpcio
    contrib/python/requests
    library/python/port_manager
    library/recipes/docker_compose/lib
    ydb/public/api/grpc
    ydb/public/api/protos
    library/python/testing/recipe
    library/python/testing/yatest_common
    ydb/public/tools/lib/cmds
    ydb/tests/library
    ydb/tests/functional/security/oidc/lib
)

END()
