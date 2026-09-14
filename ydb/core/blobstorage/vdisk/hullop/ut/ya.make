UNITTEST_FOR(ydb/core/blobstorage/vdisk/hullop)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
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
