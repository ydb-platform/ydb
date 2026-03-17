PY3_LIBRARY(django_rest_api.polls)

PEERDIR(
    contrib/python/django/django-2.2
    contrib/python/djangorestframework
    contrib/deprecated/python/coreapi

    library/python/django
    library/python/django_tools_log_context
)

PY_SRCS(
    __init__.py
    admin.py
    api.py
    apps.py
    models.py
    serializers.py
    urls.py
    views.py
)

RESOURCE_FILES(PREFIX library/python/django/example/django_rest_api/polls/
    # шаблоны
    templates/polls/base.html
    templates/polls/detail.html
    templates/polls/index.html
    templates/polls/results.html

    # фикстуры
    fixtures/00_initial_polls.json
)

END()

