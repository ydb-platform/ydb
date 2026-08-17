PY3_LIBRARY()

PEERDIR(
    contrib/python/pexpect
    contrib/python/pyarrow
    contrib/python/PyYAML
    ydb/public/api/grpc
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

PY_SRCS(
    benchmark_abstract.py
    benchmark_factory.py
    benchmark_runner.py
    common.py
    wiki_sql_dataset.py
)

STYLE_PYTHON()

END()
