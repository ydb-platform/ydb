UNITTEST_FOR(ydb/core/blobstorage/pdisk)

FORK_SUBTESTS()

OWNER(
    cthulhu
    ddoarn
    fomichev
    g:kikimr
)

IF (WITH_VALGRIND)
    ENV(VALGRIND_OPTS=--max-stackframe=16000000)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(2400)
    SPLIT_FACTOR(20)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion 
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/testlib/actors
)

SRCS(
    blobstorage_pdisk_blockdevice_ut.cpp
    blobstorage_pdisk_crypto_ut.cpp
    blobstorage_pdisk_util_ut.cpp
    blobstorage_pdisk_ut.cpp
    blobstorage_pdisk_ut_actions.cpp
    blobstorage_pdisk_ut_helpers.cpp
    blobstorage_pdisk_ut_run.cpp
)

IF (BUILD_TYPE == "RELEASE")
    SRCS(
        blobstorage_pdisk_ut_yard.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

END()
