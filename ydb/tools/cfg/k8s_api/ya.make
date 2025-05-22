PY3_LIBRARY()

PY_SRCS(
    __init__.py
    k8s_api.py
)

PEERDIR(
    contrib/python/kubernetes
)

END()
