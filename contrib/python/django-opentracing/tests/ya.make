PY3TEST()

ENV(DJANGO_SETTINGS_MODULE=settings)

PEERDIR(
    contrib/python/django-opentracing
    contrib/python/mock
    contrib/python/django/django-3
)

SRCDIR(
    contrib/python/django-opentracing/django_opentracing
)

TEST_SRCS(
    test_middleware.py
)

PY_SRCS(
    TOP_LEVEL
    __init__.py
    settings.py
    urls.py
    views.py
)

NO_LINT()

END()
