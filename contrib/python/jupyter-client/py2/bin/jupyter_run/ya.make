PY2_PROGRAM(jupyter-run)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/jupyter-client
)

PY_MAIN(jupyter_client.runapp:main)

END()
