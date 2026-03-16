PY2TEST()

PEERDIR(
    contrib/python/APScheduler
    contrib/python/gevent
    contrib/python/pymongo
    contrib/python/kazoo
    contrib/python/redis
    contrib/python/sqlalchemy/sqlalchemy-1.4
    contrib/python/pytest-tornado
    contrib/python/tornado
    contrib/python/Twisted
    contrib/python/mock
    contrib/deprecated/python/trollius
)

TEST_SRCS(
    conftest.py
    test_executors.py
    test_expressions.py
    test_job.py
    test_jobstores.py
    test_schedulers.py
    test_triggers.py
    test_util.py
)

NO_LINT()

END()
