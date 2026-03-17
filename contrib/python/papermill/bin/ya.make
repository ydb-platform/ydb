PY3_PROGRAM(papermill)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/papermill
    contrib/python/ipykernel/py3
    contrib/python/tornado
)

PY_MAIN(papermill.cli:papermill)

END()
