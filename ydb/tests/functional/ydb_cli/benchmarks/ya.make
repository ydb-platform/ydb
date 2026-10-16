PY3_PROGRAM(ydb_cli_bench)

PY_SRCS(
    MAIN main.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/functional/ydb_cli/benchmarks/impl
)

STYLE_PYTHON()

END()

RECURSE(impl)
