PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

SUBSCRIBER(g:python-contrib)

IF (PYTHON2)
    PEERDIR(contrib/python/retry/py2)
ELSE()
    PEERDIR(contrib/python/retry/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
