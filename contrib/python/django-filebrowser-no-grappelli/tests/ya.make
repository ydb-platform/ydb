PY3TEST()

ENV(DJANGO_SETTINGS_MODULE=tests.settings)

PEERDIR(
    contrib/python/django-filebrowser-no-grappelli
    contrib/python/pytest
    contrib/python/mock
    contrib/python/django/django-3
    library/python/django
    library/python/gunicorn
)

DATA(
    arcadia/contrib/python/django-filebrowser-no-grappelli/tests/tmp.db
    arcadia/contrib/python/django-filebrowser-no-grappelli/filebrowser/static
)

TEST_SRCS(
    __init__.py
    test_base.py
    test_commands.py
    test_decorators.py
    test_fields.py
    test_namers.py
    test_settings.py
    test_sites.py
    test_templatetags.py
    test_versions.py
)

PY_SRCS(
    NAMESPACE tests
    base.py
    settings.py
    urls.py
)

NO_LINT()

NO_CHECK_IMPORTS()

END()
