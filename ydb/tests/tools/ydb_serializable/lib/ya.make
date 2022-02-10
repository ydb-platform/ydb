PY3_LIBRARY() 

OWNER(g:kikimr)

PEERDIR(
    contrib/python/tornado/tornado-4
    ydb/tests/library 
    ydb/public/sdk/python/ydb 
)

PY_SRCS(__init__.py)

END()
