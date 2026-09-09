PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/nbs/suite.inc)

TEST_SRCS(
    conftest.py
    F1_15_vhost_after_tablet_restart.py
    F1_23_zero_blocks.py
)

END()
