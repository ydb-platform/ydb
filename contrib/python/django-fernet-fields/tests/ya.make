PY3TEST()

ENV(DJANGO_SETTINGS_MODULE=test.settings.sqlite)

PEERDIR(
    library/python/django
    contrib/python/django-fernet-fields
    contrib/python/pytest-django
    contrib/python/psycopg2
    contrib/python/django/django-3
)

SRCDIR(contrib/python/django-fernet-fields/fernet_fields)

PY_SRCS(
    TOP_LEVEL
    test/__init__.py
    test/models.py
    test/settings/__init__.py
    test/settings/base.py
    test/settings/pg.py
    test/settings/sqlite.py
)

TEST_SRCS(
    test/test_fields.py
)

NO_LINT()

END()
