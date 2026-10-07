PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/nbs/suite.inc)

TEST_SRCS(
    conftest.py
    F2_01_tablet_kill_under_load.py
    F2_02_slot_stop_start.py
    F2_03_one_dbg_host_down.py
    F2_04_two_dbg_hosts_down.py
    F2_05_three_dbg_hosts_down.py
    F2_06_pdisk_broken.py
    F2_07_pdisk_stop_restart.py
    F2_08_sigstop_slow_node.py
    F2_09_kill_mid_flush.py
    F2_10_kill_mid_erase.py
    F2_11_kill_mid_restore.py
    F2_14_combined_faults.py
)

END()
