PY3TEST()

TEST_SRCS(
    test_s1.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/tpc/medium/tpch/common.inc)

# Share class setup and imported data across all 22 queries.
END()
