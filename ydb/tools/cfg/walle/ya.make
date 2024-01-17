PY23_LIBRARY()

PY_SRCS(
    __init__.py
    walle.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/six
)

END()
