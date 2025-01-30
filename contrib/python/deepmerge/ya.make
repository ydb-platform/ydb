PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

SUBSCRIBER(g:python-contrib)

IF (PYTHON2)
    PEERDIR(contrib/python/deepmerge/py2)
ELSE()
    PEERDIR(contrib/python/deepmerge/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
