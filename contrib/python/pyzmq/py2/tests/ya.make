PY2TEST()

SRCDIR(
    contrib/python/pyzmq/py2
)

FORK_TESTS()

TEST_SRCS(
    zmq/tests/conftest.py
    zmq/tests/test_auth.py
    #zmq/tests/test_cffi_backend.py
    zmq/tests/test_constants.py
    zmq/tests/test_context.py
    #zmq/tests/test_cython.py
    zmq/tests/test_decorators.py
    zmq/tests/test_device.py
    zmq/tests/test_draft.py
    zmq/tests/test_error.py
    zmq/tests/test_etc.py
    zmq/tests/test_future.py
    zmq/tests/test_imports.py
    zmq/tests/test_includes.py
    zmq/tests/test_ioloop.py
    zmq/tests/test_log.py
    zmq/tests/test_message.py
    zmq/tests/test_monitor.py
    zmq/tests/test_monqueue.py
    zmq/tests/test_multipart.py
    zmq/tests/test_pair.py
    zmq/tests/test_poll.py
    zmq/tests/test_proxy_steerable.py
    zmq/tests/test_pubsub.py
    zmq/tests/test_reqrep.py
    zmq/tests/test_retry_eintr.py
    zmq/tests/test_security.py
    zmq/tests/test_socket.py
    zmq/tests/test_ssh.py
    zmq/tests/test_version.py
    #zmq/tests/test_win32_shim.py
    zmq/tests/test_z85.py
    zmq/tests/test_zmqstream.py
)

PEERDIR(
    contrib/python/pyzmq
    contrib/python/tornado
    contrib/python/gevent
)

NO_LINT()

END()
