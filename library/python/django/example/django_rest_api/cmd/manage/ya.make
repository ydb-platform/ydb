PY3_PROGRAM(manage)

PEERDIR(
    library/python/django
    library/python/django/example/django_rest_api
)

PY_SRCS(
    manage.py
)

PY_MAIN(library.python.django.example.django_rest_api.cmd.manage.manage)

NO_CHECK_IMPORTS()

END()

