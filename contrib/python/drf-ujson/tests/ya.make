PY3TEST()

PEERDIR(
    contrib/python/drf-ujson
    contrib/python/django/django-5
)

TEST_SRCS(
    tests.py
)

NO_LINT()

NO_CHECK_IMPORTS()

END()
