UNITTEST_FOR(ydb/core/blobstorage/pdisk)

FORK_SUBTESTS()

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
REQUIREMENTS(cpu:1)
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
    blobstorage_pdisk_log_cache_ut.cpp
    blobstorage_pdisk_util_ut.cpp
    blobstorage_pdisk_ut_env.cpp
    blobstorage_pdisk_ut_races.cpp
    blobstorage_pdisk_ut.cpp
    blobstorage_pdisk_ut_actions.cpp
    blobstorage_pdisk_ut_helpers.cpp
    blobstorage_pdisk_ut_run.cpp
    blobstorage_pdisk_ut_sectormap.cpp
    blobstorage_pdisk_restore_ut.cpp
    mock/pdisk_mock.cpp
)

IF (BUILD_TYPE != "DEBUG")
    SRCS(
        blobstorage_pdisk_ut_yard.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

END()
