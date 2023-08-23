PY3_LIBRARY()

PY_SRCS(
    __init__.py
    matchers.py
    tables.py
    test_base.py
    requests_client.py
)

IF (NOT PYTHON3)
    PEERDIR(
        contrib/deprecated/python/enum34
    )
ENDIF()

PEERDIR(
    ydb/tests/oss/ydb_sdk_import
    contrib/python/requests
    contrib/python/xmltodict
)

END()
