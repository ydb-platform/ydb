OWNER(g:kikimr)
PY3_PROGRAM(ttl_readtable)

PY_SRCS(
    __main__.py
    ttl.py
)

PEERDIR(
    contrib/python/iso8601
    ydb/public/sdk/python/ydb
)

END()

