PY3TEST()

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

TAG(
    ya:force_sandbox
)
REQUIREMENTS(
    cpu:16
    yav:DOCKER_TOKEN_PATH=file:sec-01e0d4agf6pfvwdjwxp61n3fvg:docker_token
)

PEERDIR(
    contrib/python/docker

    contrib/python/aiopg
    contrib/python/sqlalchemy/sqlalchemy-1.4
)

NO_LINT()

PY_SRCS(
    conftest.py
)

TEST_SRCS(
    test_async_await.py
    test_async_transaction.py
    test_connection.py
    test_cursor.py
    test_extended_types.py
    test_isolation_level.py
    test_pool.py
    test_sa_connection.py
    test_sa_cursor.py
    test_sa_default.py
    test_sa_distil.py
    test_sa_engine.py
    test_sa_priority_name.py
    test_sa_transaction.py
    test_sa_types.py
    test_transaction.py
    test_utils.py
    test_version.py
)

END()
