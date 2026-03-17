PY3TEST()

PEERDIR(
    contrib/python/aiohttp
    contrib/python/asgiref
    contrib/python/django/django-5
    contrib/python/prometheus-client
)

DATA(
    arcadia/contrib/python/prometheus-client/tests/certs
    arcadia/contrib/python/prometheus-client/tests/proc
)

TEST_SRCS(
    test_aiohttp.py
    test_asgi.py
    test_core.py
    test_django.py
    test_exposition.py
    test_gc_collector.py
    test_graphite_bridge.py
    test_multiprocess.py
    test_parser.py
    test_platform_collector.py
    test_process_collector.py
    test_samples.py
    # without twisted support
    # test_twisted.py
    test_wsgi.py
)

NO_LINT()

END()
