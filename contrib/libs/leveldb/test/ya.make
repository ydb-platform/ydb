PROGRAM(leveldb-test)

NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/libs/leveldb
)

CFLAGS(
    -DSINGLE_TEST_SUITE
)

SET(LEVELDB_DIR contrib/libs/leveldb)
SRCDIR(
    ${LEVELDB_DIR}/db
    ${LEVELDB_DIR}/helpers/memenv
    ${LEVELDB_DIR}/port
    ${LEVELDB_DIR}/table
    ${LEVELDB_DIR}/util
)

SRCS(
    testutil.cc
    testharness.cc
    testmain.cc

    arena_test.cc
    autocompact_test.cc
    bloom_test.cc
    cache_test.cc
    coding_test.cc
    corruption_test.cc
    crc32c_test.cc
    db_test.cc
    dbformat_test.cc
    env_test.cc
    fault_injection_test.cc
    filename_test.cc
    filter_block_test.cc
    hash_test.cc
    log_test.cc
    memenv_test.cc
    skiplist_test.cc
    table_test.cc
    version_edit_test.cc
    version_set_test.cc
    write_batch_test.cc
)

END()
