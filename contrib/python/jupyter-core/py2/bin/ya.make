PY2_PROGRAM(jupyter-core)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/jupyter-core
)

PY_MAIN(jupyter_core.command:main)

END()
