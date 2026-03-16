PY2_PROGRAM(jupyter-kernel)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/ipykernel
    contrib/python/jupyter-client
)

PY_MAIN(jupyter_client.kernelapp:main)

END()
