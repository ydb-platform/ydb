UNITTEST()

OWNER(g:kikimr)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

SRCS(
    block_race.cpp
    counting_events.cpp
    decommit_3dc.cpp
    defrag.cpp
    encryption.cpp
    gc_quorum_3dc.cpp
    incorrect_queries.cpp
    main.cpp
    mirror3of4.cpp
    sanitize_groups.cpp
    space_check.cpp
    sync.cpp
    replication.cpp
)

IF (BUILD_TYPE == "RELEASE")
    SRCS(
        big_cluster.cpp
        multiget.cpp
        osiris.cpp
        patch.cpp
        race.cpp
        scrub.cpp
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
    ut_donor
    ut_group_reconfiguration
    ut_osiris
    ut_replication
    ut_scrub
)
