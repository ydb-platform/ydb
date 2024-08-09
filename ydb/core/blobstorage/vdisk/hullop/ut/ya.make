UNITTEST_FOR(ydb/core/blobstorage/vdisk/hullop)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(
        ram:16
    )
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/blobstorage/pdisk
)

SRCS(
    blobstorage_hullcompactdeferredqueue_ut.cpp
    blobstorage_readbatch_ut.cpp
    hullop_delayedresp_ut.cpp
)

END()
