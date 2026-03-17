PY3TEST()

FORK_TESTS()

PY_SRCS(
    TOP_LEVEL
    zmq_test_utils.py
)

TEST_SRCS(
    conftest.py
    test_asyncio.py
    test_auth.py
    #test_cffi_backend.py
    test_constants.py
    test_context.py
    #test_cython.py
    test_decorators.py
    test_device.py
    test_draft.py
    test_error.py
    test_etc.py
    test_ext.py
    test_future.py
    test_imports.py
    test_includes.py
    test_ioloop.py
    test_log.py
    test_message.py
    test_monitor.py
    test_monqueue.py
    test_multipart.py
    #test_mypy.py
    test_pair.py
    test_poll.py
    test_proxy_steerable.py
    test_pubsub.py
    test_reqrep.py
    test_retry_eintr.py
    test_security.py
    test_socket.py
    test_ssh.py
    test_version.py
    #test_win32_shim.py
    test_z85.py
    test_zmqstream.py
)

PEERDIR(
    contrib/python/pyzmq
    contrib/python/tornado
    contrib/python/gevent
    contrib/python/pytest-asyncio
)

NO_LINT()

END()
