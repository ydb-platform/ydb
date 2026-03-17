PY3TEST()

NO_LINT()

ENV(DJANGO_SETTINGS_MODULE=grappelli.tests.test_settings)

PEERDIR(
    library/python/django
    contrib/python/django/django-5
    contrib/python/django-grappelli
    contrib/python/pytest-django
    contrib/python/tzdata
)

SRCDIR(contrib/python/django-grappelli/grappelli/tests)

PY_SRCS(
    NAMESPACE grappelli.tests
    __init__.py
    admin.py
    migrations/0001_initial.py
    migrations/__init__.py
    models.py
    test_settings.py
    urls.py
)

TEST_SRCS(
    test_checks.py
    test_dashboard.py
    test_related.py
    test_switch.py
)

END()
