PY23_LIBRARY()

PEERDIR(
    contrib/python/requests
    contrib/python/retry
)

PY_SRCS(
    __init__.py
    client.py
)

END()
