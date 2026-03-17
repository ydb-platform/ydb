PY2TEST()

PEERDIR(
    contrib/python/django-environ
)

DATA(
    arcadia/contrib/python/django-environ/py2/environ/test_env.txt
)

SRCDIR(
    contrib/python/django-environ/py2/environ
)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
