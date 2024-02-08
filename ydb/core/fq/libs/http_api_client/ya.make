PY3_LIBRARY()

OWNER(g:yq)

PY_SRCS(
    http_client.py
)

PEERDIR(
    contrib/python/requests
)

END()
