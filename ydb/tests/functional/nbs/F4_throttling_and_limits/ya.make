PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/nbs/suite.inc)

TEST_SRCS(
    conftest.py
    F4_06_cleaningup_force_flush.py
    F4_07_flush_cooldown_on_errors.py
    F4_09_overloaded_pending_queue.py
    F4_10_write_over_512kib.py
    F4_12_no_user_iops_throttle.py
)

END()
