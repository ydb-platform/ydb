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
    encryption.cpp
    extra_block_checks.cpp
    gc_quorum_3dc.cpp
    group_reconfiguration.cpp
    incorrect_queries.cpp
    main.cpp
    mirror3of4.cpp
    sanitize_groups.cpp
    scrub_fast.cpp
    snapshots.cpp
    space_check.cpp
    sync.cpp
)

IF (BUILD_TYPE == "RELEASE")
    SRCS(
        big_cluster.cpp
        get.cpp
        discover.cpp
        multiget.cpp
        patch.cpp
        recovery.cpp
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
    ut_blob_depot
    ut_blob_depot_fat
    ut_donor
    ut_group_reconfiguration
    ut_osiris
    ut_replication
    ut_scrub
)
