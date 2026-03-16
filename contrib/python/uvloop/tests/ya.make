PY3TEST()

FORK_TESTS()

PEERDIR(
    contrib/python/aiohttp
    contrib/python/uvloop
    contrib/python/pyOpenSSL
)

DATA(
    arcadia/contrib/python/uvloop/tests/certs
)

TEST_SRCS(
    __init__.py
    # __main__.py
    test_aiohttp.py
    test_base.py
    test_context.py
    test_cython.py
    # test_dealloc.py
    test_dns.py
    test_executors.py
    test_fs_event.py
    test_libuv_api.py
    test_pipes.py
    # test_process.py
    # test_process_spawning.py
    test_regr1.py
    test_runner.py
    # test_signals.py
    test_sockets.py
    # test_sourcecode.py
    test_tcp.py
    test_testbase.py
    test_udp.py
    test_unix.py
)

NO_LINT()

END()
