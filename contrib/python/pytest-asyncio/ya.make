PY3_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

# TODO: replace with new
PEERDIR(contrib/python/pytest-asyncio/pytest-asyncio-0.21.1)

NO_LINT()

END()

RECURSE(
    pytest-asyncio-0.21.1
    pytest-asyncio-new
)
