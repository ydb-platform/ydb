UNITTEST()

FORK_SUBTESTS()

REQUIREMENTS(ram:32 cpu:2)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    acceleration.cpp
    assimilation.cpp
    backpressure.cpp
    block_race.cpp
    bsc_cache.cpp
    cancellation.cpp
    counting_events.cpp
    corrupted_reads.cpp
    deadlines.cpp
    decommit_3dc.cpp
    defrag.cpp
    discover.cpp
    ds_proxy_lwtrace.cpp
    encryption.cpp
    extra_block_checks.cpp
    gc.cpp
    gc_quorum_3dc.cpp
    get.cpp
    get_block.cpp
    group_reconfiguration.cpp
    incorrect_queries.cpp
    index_restore_get.cpp
    main.cpp
    mirror3dc.cpp
    mirror3of4.cpp
    monitoring.cpp
    multiget.cpp
    patch.cpp
    recovery.cpp
    retro_tracing.cpp
    sanitize_groups.cpp
    scrub_fast.cpp
    self_heal.cpp
    shred.cpp
    snapshots.cpp
    space_check.cpp
    sync.cpp
    validation.cpp
    vdisk_internals.cpp
    vdisk_malfunction.cpp
    group_size_in_units.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/scrub
)

END()

RECURSE_FOR_TESTS(
    ut_balancing
    ut_blob_depot
    ut_blob_depot_fat
    ut_bridge
    ut_check_integrity
    ut_comp_defrag
    ut_ddisk
    ut_donor
    ut_group_reconfiguration
    ut_huge
    ut_read_only_vdisk
    ut_osiris
    ut_phantom_blobs
    ut_replication
    ut_scrub
    ut_statestorage
    ut_vdisk_restart
    ut_restart_pdisk
    ut_read_only_pdisk
    ut_stop_pdisk
    ut_cluster_balancing
    ut_move_pdisk
)
