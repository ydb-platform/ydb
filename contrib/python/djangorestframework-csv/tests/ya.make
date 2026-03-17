PY3TEST()

ENV(DJANGO_SETTINGS_MODULE=contrib.python.djangorestframework-csv.tests.settings)

PEERDIR(
    contrib/python/djangorestframework-csv
    contrib/python/pytest-django
    contrib/python/django/django-4
)

DATA(
    arcadia/contrib/python/djangorestframework-csv/rest_framework_csv/testfixtures
)

SRCDIR(contrib/python/djangorestframework-csv/rest_framework_csv)

PY_SRCS(
    settings.py
)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
