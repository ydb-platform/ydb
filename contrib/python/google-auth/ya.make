PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

IF (PYTHON2)
    PEERDIR(contrib/python/google-auth/py2)
ELSE()
    PEERDIR(contrib/python/google-auth/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
