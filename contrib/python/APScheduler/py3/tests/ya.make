PY3TEST()

PEERDIR(
    contrib/python/APScheduler
    contrib/python/gevent
    contrib/python/pymongo
    contrib/python/kazoo
    contrib/python/redis
    contrib/python/sqlalchemy/sqlalchemy-2
    contrib/python/pytest-tornado
    contrib/python/tornado
    contrib/python/Twisted
    contrib/python/pytest-asyncio
    contrib/python/pytz
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
