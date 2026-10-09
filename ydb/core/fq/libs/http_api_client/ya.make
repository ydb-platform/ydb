PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    http_client.py
    query_results.py
)

PEERDIR(
    contrib/python/requests
)

END()
