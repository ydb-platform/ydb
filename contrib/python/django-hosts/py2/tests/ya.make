PY2TEST()

ENV(DJANGO_SETTINGS_MODULE=tests.settings)

PEERDIR(
    library/python/django
    contrib/python/django-hosts
    contrib/python/pytest-django
    contrib/python/django/django-1.11
)

PY_SRCS(
    NAMESPACE tests
    __init__.py
    broken_module.py
    hosts/__init__.py
    hosts/appended.py
    hosts/blank.py
    hosts/multiple.py
    hosts/simple.py
    models.py
    settings.py
    urls/complex.py
    urls/multiple.py
    urls/root.py
    urls/simple.py
    views.py
)

TEST_SRCS(
    base.py
    test_defaults.py
    test_middleware.py
    test_resolvers.py
    test_sites.py
    test_templatetags.py
    test_utils.py
)

NO_LINT()

NO_CHECK_IMPORTS(
    tests.*
)

END()
