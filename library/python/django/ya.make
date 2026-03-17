PY23_LIBRARY()

PEERDIR(
    contrib/python/six
    devtools/ya/yalibrary/makelists
)

PY_SRCS(
    __init__.py
    utils.py
    contrib/__init__.py
    contrib/staticfiles/__init__.py
    contrib/staticfiles/finders.py
    contrib/staticfiles/storage.py
    template/__init__.py
    template/loaders/__init__.py
    template/loaders/resource.py
    template/loaders/app_resource.py
    template/backends/arcadia.py
    template/backends/forms_renderer.py
)

IF (PYTHON3)
    PY_SRCS(locales.py)
ENDIF()

END()

RECURSE(
    example/django_rest_api
)
