UNITTEST_FOR(ydb/core/blobstorage/groupinfo)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/groupinfo
    ydb/core/erasure
)

SRCS(
    blobstorage_groupinfo_iter_ut.cpp
    blobstorage_groupinfo_ut.cpp
)

IF (BUILD_TYPE != "DEBUG")
    SRCS(
        blobstorage_groupinfo_blobmap_ut.cpp
        blobstorage_groupinfo_partlayout_ut.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

END()
