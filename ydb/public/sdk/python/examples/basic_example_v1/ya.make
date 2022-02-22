OWNER(g:kikimr)
PY3_PROGRAM(basic_example)

PY_SRCS(
    __main__.py
    basic_example.py
    basic_example_data.py
)

PEERDIR(
    contrib/python/iso8601
    contrib/python/requests
    ydb/public/sdk/python/ydb
)

END()

