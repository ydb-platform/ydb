PY3TEST()

TEST_SRCS(
    test_ir.py
    test_limit.py
    test_olap_filter.py
    test_sort.py
    test_scalar.py
    test_smt.py
    test_verify.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

END()
