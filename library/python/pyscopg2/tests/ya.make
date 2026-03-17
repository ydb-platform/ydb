PY3TEST()


SIZE(MEDIUM)

PEERDIR(
    library/python/pyscopg2
    contrib/python/asyncpg
    contrib/python/sqlalchemy/sqlalchemy-1.4
)

NO_CHECK_IMPORTS(
)

PY_SRCS(
    conftest.py
    __init__.py
    mocks/__init__.py
    mocks/pool_manager.py
)

TEST_SRCS(
    test_aiopg.py
    test_aiopg_sa.py
    test_asyncpg.py
    test_balancer_policy.py
    test_base_pool_manager.py
    test_trouble.py
    test_utils.py
)

END()
