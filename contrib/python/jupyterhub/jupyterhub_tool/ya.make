PY3_PROGRAM(jupyterhub)

VERSION(Service-proxy-version)

LICENSE(BSD)

PEERDIR(
    contrib/python/jupyterhub
    contrib/python/jupyterhub/jupyterhub/alembic
)

NO_LINT()

PY_SRCS(
    __main__.py
)

END()

