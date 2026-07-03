PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    common.py
)

PEERDIR(
    library/python/testing/yatest_common
    ydb/tests/tools/fq_runner
)

END()

