UNITTEST()

FORK_SUBTESTS(MODULO)

SPLIT_FACTOR(20)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/actors/core
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/ut_vdisk/lib
    ydb/core/blobstorage/vdisk/common
    ydb/core/blobstorage/vdisk/query
    ydb/core/testlib/default
    ydb/core/testlib/actors
    ydb/core/testlib/basics
)

YQL_LAST_ABI_VERSION()

SRCS(
    dsproxy_put_ut.cpp
    dsproxy_quorum_tracker_ut.cpp
    dsproxy_sequence_ut.cpp
    dsproxy_patch_ut.cpp
    dsproxy_counters_ut.cpp
)


IF (BUILD_TYPE == "RELEASE")
    SRCS(
        dsproxy_fault_tolerance_ut.cpp
        dsproxy_get_ut.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

END()
