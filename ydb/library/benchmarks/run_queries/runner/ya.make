PY3_LIBRARY()

PY_SRCS(
    runner.py
    s3_utils.py
)

PEERDIR(
    ydb/library/benchmarks/report
    ydb/library/benchmarks/template
)

END()