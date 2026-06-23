PY3_PROGRAM(solomon_client)

PEERDIR(
    contrib/python/requests
    contrib/python/retry
)

PY_SRCS(
    MAIN main.py
)

END()
