PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

IF (PYTHON2)
    ENV(PYTHON2_YDB_IMPORT='yes')
    PEERDIR(ydb/public/sdk/python)
ELSE()
    PEERDIR(ydb/public/sdk/python)
ENDIF()

PEERDIR(
    ydb/tests/oss/canonical
    ydb/public/api/client/yc_public/iam
)

END()
