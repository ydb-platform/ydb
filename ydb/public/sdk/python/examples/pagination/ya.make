OWNER(g:kikimr mbabich)
PY3_PROGRAM(pagination)

PY_SRCS(
    __main__.py
    pagination.py
    sample_data.py
)

PEERDIR(
    ydb/public/sdk/python/ydb
)

END()

