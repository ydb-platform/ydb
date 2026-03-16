PY3_PROGRAM(ipykernel_launcher)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/ipykernel
    contrib/python/tornado
)

PY_MAIN(ipykernel_launcher:main)

END()
