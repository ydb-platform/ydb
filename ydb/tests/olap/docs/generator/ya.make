PY3_PROGRAM()

PY_SRCS(
    parser.py
    __main__.py
)

REQUIREMENTS(cpu:2)
PEERDIR(
    ydb/tests/olap/scenario/helpers
    contrib/python/PyYAML
)

END()
