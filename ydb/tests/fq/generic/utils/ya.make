PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    endpoint_determiner.py
    settings.py
    ydb_helpers.py
)

PEERDIR(
    library/python/testing/yatest_common
)

END()
