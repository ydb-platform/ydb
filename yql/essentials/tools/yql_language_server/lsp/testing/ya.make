PY3_LIBRARY()

PEERDIR(
    contrib/python/pydantic/pydantic-2
)

PY_SRCS(
    differential.py
    io.py
    replay.py
    server.py
    trace.py
)

END()

RECURSE_FOR_TESTS(
    test
)
