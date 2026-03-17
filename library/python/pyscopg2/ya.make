PY3_LIBRARY()


VERSION(0.6.1+gb6d25c8)


PEERDIR(
    contrib/python/aiopg
    contrib/python/async-timeout
    contrib/python/psycopg2
    contrib/python/asyncpg
    contrib/python/mock
)

PY_SRCS(
    TOP_LEVEL
    pyscopg2/__init__.py
    pyscopg2/aiopg.py
    pyscopg2/aiopg_sa.py
    pyscopg2/asyncpg.py
    pyscopg2/balancer_policy/__init__.py
    pyscopg2/balancer_policy/base.py
    pyscopg2/balancer_policy/greedy.py
    pyscopg2/balancer_policy/random_weighted.py
    pyscopg2/balancer_policy/round_robin.py
    pyscopg2/base.py
    pyscopg2/utils.py
    pyscopg2/version.py
)

RESOURCE_FILES(
    PREFIX contrib/python/pyscopg2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(tests)
