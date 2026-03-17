PY3TEST()

PEERDIR(
    contrib/python/freezegun
    contrib/python/greenlet
    contrib/python/pretend
    contrib/python/pytest-asyncio
    contrib/python/structlog
    contrib/python/Twisted
)

TEST_SRCS(
    __init__.py
    additional_frame.py
    conftest.py
    helpers.py
    processors/__init__.py
    processors/test_processors.py
    # processors/test_renderers.py - need time_machine
    test_base.py
    test_config.py
    test_contextvars.py
    test_dev.py
    test_frames.py
    # test_generic.py - need time_machine
    test_output.py
    test_packaging.py
    test_stdlib.py
    test_testing.py
    test_threadlocal.py
    # test_tracebacks.py
    test_twisted.py
    test_utils.py
    # typing/api.py
)

NO_LINT()

END()
