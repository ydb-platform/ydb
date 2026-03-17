PY3_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

PEERDIR(
    contrib/python/minijinja/py3
    contrib/python/minijinja/rust
)

END()

RECURSE(
    py3
)
