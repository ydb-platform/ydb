OWNER(g:kikimr)
PY3_PROGRAM(secondary_indexes_builtin)

PY_SRCS(
    __main__.py
    secondary_indexes_builtin.py
)

PEERDIR(
    contrib/python/iso8601
    ydb/public/sdk/python/ydb
)

END()

