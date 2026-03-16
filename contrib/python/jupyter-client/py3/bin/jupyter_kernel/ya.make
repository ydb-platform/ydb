PY3_PROGRAM(jupyter-kernel)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/ipykernel
    contrib/python/jupyter-client
    contrib/python/tornado/tornado-6
)

PY_MAIN(jupyter_client.kernelapp:main)

END()
