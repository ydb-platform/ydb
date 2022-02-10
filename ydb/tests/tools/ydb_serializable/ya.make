PY3_PROGRAM() 

OWNER(g:kikimr)

PEERDIR(
    ydb/tests/tools/ydb_serializable/lib 
)

PY_SRCS(__main__.py)

END()
