PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/celery/py2)
ELSE()
    PEERDIR(contrib/python/celery/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
