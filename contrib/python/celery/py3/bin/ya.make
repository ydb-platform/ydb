PY3_PROGRAM(celery)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/celery
)

PY_MAIN(celery.__main__:main)

END()
