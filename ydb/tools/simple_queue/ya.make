OWNER(g:kikimr)
PY2_PROGRAM(simple_queue)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/ydb
    library/python/monlib
)

END()

