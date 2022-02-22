PY23_LIBRARY()

OWNER(g:kikimr)

PY_SRCS(
    __init__.py
    tables.py
)

IF (NOT PYTHON3)
    PEERDIR(
        contrib/python/enum34
    )
ENDIF()

PEERDIR(
    ydb/public/sdk/python/ydb
)

END()
