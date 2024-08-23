PY3TEST()

FORK_SUBTESTS()

SPLIT_FACTOR(50)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

PEERDIR(
    ydb/public/api/protos
    ydb/public/api/grpc
    ydb/tests/tools/datastreams_helpers
    ydb/tests/tools/fq_runner
)

DEPENDS(
    ydb/tests/tools/pq_read
)

PY_SRCS(
    conftest.py
    test_base.py
)

TEST_SRCS(
    test_2_selects_limit.py
    test_3_selects.py
    test_bad_syntax.py
    test_big_state.py
    test_continue_mode.py
    test_cpu_quota.py
    test_delete_read_rules_after_abort_by_system.py
    test_eval.py
    test_invalid_consumer.py
    test_kill_pq_bill.py
    test_mem_alloc.py
    test_metrics_cleanup.py
    test_pq_read_write.py
    test_public_metrics.py
    test_read_rules_deletion.py
    test_recovery.py
    test_recovery_match_recognize.py
    test_recovery_mz.py
    test_restart_query.py
    test_row_dispatcher.py
    test_select_1.py
    test_select_limit_db_id.py
    test_select_limit.py
    test_select_timings.py
    test_stop.py
    test_watermarks.py
    test_yds_bindings.py
    test_yq_streaming.py
)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(200)
    SIZE(MEDIUM)
ENDIF()

END()
