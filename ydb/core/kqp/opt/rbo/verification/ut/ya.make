PY3TEST()

SIZE(MEDIUM)

TEST_SRCS(
    test_cardinality_projection.py
    test_checked_concat.py
    test_cli_budget.py
    test_integral_average.py
    test_ir.py
    test_analysis.py
    test_join.py
    test_key_only_aggregate.py
    test_decimal.py
    test_diagnostics.py
    test_encoding_strategies.py
    test_limit.py
    test_logical_reference.py
    test_olap_filter.py
    test_project_error.py
    test_sort.py
    test_scalar.py
    test_sql_in.py
    test_stage_compaction.py
    test_stage_ports.py
    test_stagegraph_reference.py
    test_string_order.py
    test_string_proof.py
    test_subplans.py
    test_union_all_variadic.py
    test_smt.py
    test_stable_pickle.py
    test_temporal.py
    test_verify.py
    test_window_avg.py
    test_window_rank.py
    test_window_rows.py
    test_window_sum.py
)

PEERDIR(
    ydb/core/kqp/opt/rbo/verification
)

DEPENDS(
    contrib/tools/z3
)

END()
