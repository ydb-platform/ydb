UNITTEST()

FORK_SUBTESTS(MODULO)

SPLIT_FACTOR(20)

REQUIREMENTS(
    cpu:1
    ram:32
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/blobstorage/dsproxy
#    ydb/core/blobstorage/ut_vdisk/lib
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    dsproxy_put_ut.cpp
    dsproxy_quorum_tracker_ut.cpp
    dsproxy_sequence_ut.cpp
    dsproxy_patch_ut.cpp
    dsproxy_counters_ut.cpp
)

IF (BUILD_TYPE != "DEBUG")
    SRCS(
        dsproxy_get_ut.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

END()
