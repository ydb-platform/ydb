PY3TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/asgiref
    contrib/python/Flask
    contrib/python/Flask-Caching
    contrib/python/pytest-xprocess
    contrib/python/redis
)

DATA(
    arcadia/contrib/python/Flask-Caching/py3/tests/test_template.html
)

PY_SRCS(
    conftest.py
)

TEST_SRCS(
    test_backend_cache.py
    test_basic_app.py
    test_cache.py
    # test_init.py
    test_memoize.py
    test_templates.py
    test_view.py
)

NO_LINT()

END()
