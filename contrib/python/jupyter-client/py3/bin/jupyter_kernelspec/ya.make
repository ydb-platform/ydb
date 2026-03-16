PY3_PROGRAM(jupyter-kernelspec)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/jupyter-client
    contrib/python/tornado
)

PY_MAIN(jupyter_client.kernelspecapp:main)

END()
