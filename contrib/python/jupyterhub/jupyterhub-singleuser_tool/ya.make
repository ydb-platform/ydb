PY3_PROGRAM(jupyterhub-singleuser)

VERSION(Service-proxy-version)

LICENSE(BSD)

PEERDIR(
    contrib/python/jupyterhub
)

NO_LINT()

PY_SRCS(
    __main__.py
)

END()
