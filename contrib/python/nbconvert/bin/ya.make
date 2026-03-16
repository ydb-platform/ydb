PY3_PROGRAM(jupyter-nbconvert)

VERSION(Service-proxy-version)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/nbconvert
    contrib/python/ipython
    contrib/python/tornado
)

NO_LINT()

PY_MAIN(nbconvert.nbconvertapp:main)

END()
