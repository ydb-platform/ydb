PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/coverage/py2)
ELSE()
    PEERDIR(contrib/python/coverage/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    plugins
    py2
    py3
)
