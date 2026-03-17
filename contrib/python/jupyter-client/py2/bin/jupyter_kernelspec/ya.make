PY2_PROGRAM(jupyter-kernelspec)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/jupyter-client
)

PY_MAIN(jupyter_client.kernelspecapp:main)

END()
