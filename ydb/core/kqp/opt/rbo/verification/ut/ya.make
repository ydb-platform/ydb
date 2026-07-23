PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    test_ir.py
    test_decimal.py
    test_limit.py
    test_logical_reference.py
    test_olap_filter.py
    test_sort.py
    test_scalar.py
    test_sql_in.py
    test_stage_compaction.py
    test_stagegraph_reference.py
    test_string_order.py
    test_string_proof.py
    test_subplans.py
    test_smt.py
    test_verify.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

DEPENDS(
    contrib/tools/z3
)

END()
