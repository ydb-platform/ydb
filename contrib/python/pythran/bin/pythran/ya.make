PY3_PROGRAM(pythran)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/pythran
)

PY_MAIN(pythran.run:run)

END()
