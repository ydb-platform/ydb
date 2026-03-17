PY3_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

PEERDIR(
    contrib/python/ryaml/py3
    contrib/python/ryaml/rust
)

END()

RECURSE(
    py3
)
