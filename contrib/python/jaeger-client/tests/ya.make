PY3TEST()

FORK_TESTS()

PEERDIR(
    contrib/python/jaeger-client
    contrib/python/mock
    contrib/python/pytest-asyncio
    contrib/python/pytest-localserver
    contrib/python/pytest-tornado
    contrib/python/tornado
    contrib/python/pycurl
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_TUDPTransport.py
    test_api.py
    test_codecs.py
    test_config.py
    test_local_agent_net.py
    test_metrics.py
    test_noop_tracer.py
    test_rate_limiter.py
    test_reporter.py
    test_sampler.py
    test_span.py
    test_span_context.py
    test_thrift.py
    test_throttler.py
    test_traceback_log.py
    test_tracer.py
    test_utils.py
)

NO_LINT()

END()
