PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

IF (PYTHON2)
    PEERDIR(contrib/python/ydb/py2)
ELSE()
    PEERDIR(contrib/python/ydb/py3)
ENDIF()

PEERDIR(
  contrib/ydb/public/api/grpc
  contrib/ydb/public/api/grpc/draft
)

NO_LINT()

END()

RECURSE(
    py2
    py3
)