PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/nbs/suite.inc)

TEST_SRCS(
    conftest.py
    F1_07_delete_during_io.py
    F1_08_recreate_after_delete.py
    F1_09_write_across_stripe.py
    F1_10_write_across_vchunk.py
    F1_11_write_across_region.py
    F1_12_never_written_reads_zero.py
    F1_13_block_sizes.py
    F1_17_noisy_neighbour.py
    F1_24_delete_partition_wipe.py
    F1_25_max_disk_size.py
)

END()
