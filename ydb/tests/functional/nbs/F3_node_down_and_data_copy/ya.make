PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/nbs/suite.inc)

# Host-loss cases plus tablet-kill cases run longer than the shared 600s
# chunk when the machine is also running F1/F2.
TIMEOUT(900)

TEST_SRCS(
    conftest.py
    F3_03_offline_promotes_handoff.py
    F3_04_offline_query_addhost.py
    F3_05_ondiskbroken_stays_broken.py
    F3_06_copier_starts_after_promotion.py
    F3_07_serial_1mib_ranges.py
    F3_08_fresh_ddisk_not_readable_above_watermark.py
    F3_09_copy_complete_watermark_unset.py
    F3_12_user_writes_during_copy.py
    F3_15_copy_does_not_resume_incrementally.py
    F3_16_dostart_does_not_start_copiers.py
    F3_18_ahead_not_readable_still_copied.py
    F3_19_no_demote_no_rebalance.py
)

END()
