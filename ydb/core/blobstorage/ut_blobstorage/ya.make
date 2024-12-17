UNITTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    acceleration.cpp
    assimilation.cpp
    block_race.cpp
    counting_events.cpp
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
    sanitize_groups.cpp
    scrub_fast.cpp
    snapshots.cpp
    space_check.cpp
    sync.cpp
    ut_helpers.cpp
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
    ut_donor
    ut_group_reconfiguration
    ut_huge
    ut_read_only_vdisk
    ut_osiris
    ut_replication
    ut_scrub
    ut_vdisk_restart
    ut_restart_pdisk
    ut_read_only_pdisk
)
