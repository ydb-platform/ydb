PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(ydb/public/sdk/python2)
ELSE()
    PEERDIR(ydb/public/sdk/python3)
ENDIF()

END()
