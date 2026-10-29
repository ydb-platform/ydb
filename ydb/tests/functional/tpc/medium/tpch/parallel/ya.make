PY3TEST()

TEST_SRCS(
    test_parallel.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/tpc/medium/tpch/common.inc)

# Do not fork subtests: setup_class runs the complete parallel workload.
END()
