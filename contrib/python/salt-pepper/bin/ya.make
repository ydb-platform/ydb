PY3_PROGRAM(pepper)

VERSION(Service-proxy-version)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/salt-pepper
)

PY_SRCS(__main__.py)

NO_LINT()

END()
