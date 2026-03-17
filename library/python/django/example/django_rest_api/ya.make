PY3_LIBRARY(library.python.django.example.django_rest_api.polls)

PEERDIR(
    contrib/python/django/django-2.2
    contrib/python/gunicorn

    library/python/django
    library/python/django_tools_log_context

    library/python/django/example/django_rest_api/polls
    library/python/django/example/django_rest_api/polls/migrations
)

PY_SRCS(
    settings.py
)

RESOURCE_FILES(
    # фикстуры
    fixtures/initial_fixture.json
)

END()

RECURSE(
    polls
    cmd
)
