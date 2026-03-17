PY3TEST()

PEERDIR(
    contrib/python/mock
    contrib/deprecated/python/opentracing
    contrib/python/gevent
    contrib/python/tornado
)

TEST_SRCS(
    __init__.py
    conftest.py
    ext/__init__.py
    mocktracer/__init__.py
    mocktracer/test_api.py
    mocktracer/test_propagation.py
    mocktracer/test_span.py
    mocktracer/test_tracer.py
    scope_managers/__init__.py
    scope_managers/test_gevent.py
    scope_managers/test_threadlocal.py
    scope_managers/test_tornado.py
    test_api.py
    test_api_check_mixin.py
    test_globaltracer.py
    test_noop_span.py
    test_noop_tracer.py
    test_scope.py
    test_scope_check_mixin.py
    test_scope_manager.py
)

IF (PYTHON3)
    TEST_SRCS(
        scope_managers/test_asyncio.py
        scope_managers/test_contextvars.py
    )
ENDIF()

NO_LINT()

END()
