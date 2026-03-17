PY23_LIBRARY()

VERSION(1.1.3)

PEERDIR(
    contrib/python/cached-property
    contrib/python/requests
    contrib/python/monotonic
    contrib/python/six
    library/python/ylog
)

PY_SRCS(
    TOP_LEVEL
    django_tools_log_context/__init__.py
    django_tools_log_context/apps.py
    django_tools_log_context/celery.py
    django_tools_log_context/creator.py
    django_tools_log_context/dbtracking.py
    django_tools_log_context/http.py
    django_tools_log_context/httptracking.py
    django_tools_log_context/profiler.py
    django_tools_log_context/provider/__init__.py
    django_tools_log_context/provider/auth.py
    django_tools_log_context/provider/b2b.py
    django_tools_log_context/provider/base.py
    django_tools_log_context/provider/endpoint.py
    django_tools_log_context/provider/request.py
    django_tools_log_context/redistracking.py
    django_tools_log_context/settings.py
    django_tools_log_context/state.py
    django_tools_log_context/tracking.py
    django_tools_log_context/utils.py
    django_tools_log_context/wsgi.py
)

END()
