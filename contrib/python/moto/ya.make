PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/moto/py2)
ELSE()
    PEERDIR(contrib/python/moto/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    bin
    py2
    py3
)
