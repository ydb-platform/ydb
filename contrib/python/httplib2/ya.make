PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

IF (PYTHON2)
    PEERDIR(contrib/python/httplib2/py2)
ELSE()
    PEERDIR(contrib/python/httplib2/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
