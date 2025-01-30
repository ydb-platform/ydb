PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

IF (PYTHON2)
    ENV(PYTHON2_YDB_IMPORT='yes')
ENDIF()

PEERDIR(
    contrib/python/PyJWT
    ydb/tests/oss/canonical
    ydb/public/sdk/python
)

END()
