PY3_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

PEERDIR(
    contrib/python/tiktoken/py3
    contrib/python/tiktoken/rust
)

END()

RECURSE(
    py3
)
