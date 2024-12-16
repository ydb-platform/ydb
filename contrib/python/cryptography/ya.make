PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/cryptography/py2)
ELSE()
    PEERDIR(contrib/python/cryptography/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)

IF (OS_LINUX AND MUSL)
    RECURSE(next)
ENDIF()
