UNITTEST_FOR(ydb/core/blobstorage/pdisk)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    ENV(VALGRIND_OPTS=--max-stackframe=16000000)
ENDIF()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SPLIT_FACTOR(20)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/testlib/actors
)

IF (YDB_ENABLE_PDISK_SHRED)
    CFLAGS(
        -DENABLE_PDISK_SHRED
    )
ENDIF()

IF (YDB_DISABLE_PDISK_ENCRYPTION)
    CFLAGS(
        -DDISABLE_PDISK_ENCRYPTION
    )
ENDIF()

SRCS(
    blobstorage_pdisk_blockdevice_ut.cpp
    blobstorage_pdisk_crypto_ut.cpp
    blobstorage_pdisk_log_cache_ut.cpp
    blobstorage_pdisk_restore_ut.cpp
    blobstorage_pdisk_scheduler_ut.cpp
    blobstorage_pdisk_ut.cpp
    blobstorage_pdisk_ut_actions.cpp
    blobstorage_pdisk_ut_color_limits.cpp
    blobstorage_pdisk_ut_context.cpp
    blobstorage_pdisk_ut_env.cpp
    blobstorage_pdisk_ut_helpers.cpp
    blobstorage_pdisk_ut_races.cpp
    blobstorage_pdisk_ut_run.cpp
    blobstorage_pdisk_ut_sectormap.cpp
    blobstorage_pdisk_util_ut.cpp
    blobstorage_pdisk_ut_pdisk_config.cpp
    blobstorage_pdisk_ut_chunk_tracker.cpp
    blobstorage_pdisk_ut_failure.cpp
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
