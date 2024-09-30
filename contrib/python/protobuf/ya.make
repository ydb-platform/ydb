PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

VERSION(Service-proxy-version)

IF (PYTHON2)
    PEERDIR(contrib/python/protobuf/py2)
ELSE()
    PEERDIR(contrib/python/protobuf/py3)
ENDIF()

END()

RECURSE(
    py2
    py3
)
