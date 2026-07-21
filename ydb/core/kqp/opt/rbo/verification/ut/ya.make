PY3TEST()

TEST_SRCS(
    test_ir.py
    test_limit.py
    test_sort.py
    test_smt.py
    test_verify.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

END()
