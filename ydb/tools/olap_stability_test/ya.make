PY3_PROGRAM(olap_stability_test)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/public/sdk/python
    library/python/monlib
)

END()