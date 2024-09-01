SUBSCRIBER(g:kikimr)
PY3_PROGRAM(nemesis)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/tools/nemesis/library
    ydb/tools/cfg
)

END()
