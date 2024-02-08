PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

IF (PYTHON2)
    PEERDIR(contrib/python/cycler/py2)
ELSE()
    PEERDIR(contrib/python/cycler/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
