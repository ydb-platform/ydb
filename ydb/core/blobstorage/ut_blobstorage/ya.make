UNITTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    assimilation.cpp
    block_race.cpp
    counting_events.cpp
    decommit_3dc.cpp
    defrag.cpp
    ds_proxy_lwtrace.cpp
    encryption.cpp
    extra_block_checks.cpp
    gc.cpp
    gc_quorum_3dc.cpp
    group_reconfiguration.cpp
    incorrect_queries.cpp
    index_restore_get.cpp
    main.cpp
    mirror3dc.cpp
    mirror3of4.cpp
    monitoring.cpp
    recovery.cpp
    sanitize_groups.cpp
    scrub_fast.cpp
    snapshots.cpp
    space_check.cpp
    sync.cpp
    ut_helpers.cpp
)

IF (BUILD_TYPE != "DEBUG")
    SRCS(
#        big_cluster.cpp
        get.cpp
        discover.cpp
        multiget.cpp
        patch.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/ut_blobstorage/lib
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/scrub
)

REQUIREMENTS(ram:32)

END()

RECURSE_FOR_TESTS(
    ut_balancing
    ut_blob_depot
    ut_blob_depot_fat
    ut_donor
    ut_group_reconfiguration
    ut_read_only_vdisk
    ut_osiris
    ut_replication
    ut_scrub
    ut_vdisk_restart
)
