LIBRARY()

LICENSE(BSD-3-Clause)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.18)


NO_COMPILER_WARNINGS()

PEERDIR(
    contrib/libs/snappy
    library/cpp/deprecated/mapped_file
    library/cpp/logger
    library/cpp/digest/crc32c
)

SET(LEVELDB_DIR contrib/libs/leveldb)

SRCDIR(
    ${LEVELDB_DIR}/db
    ${LEVELDB_DIR}/helpers/memenv
    ${LEVELDB_DIR}/port
    ${LEVELDB_DIR}/table
    ${LEVELDB_DIR}/util
)

ADDINCL(GLOBAL contrib/libs/leveldb)

SRCS(
    arena.cc
    block.cc
    block_builder.cc
    bloom.cc
    builder.cc
    cache.cc
    coding.cc
    comparator.cc
    db_impl.cc
    db_iter.cc
    dbformat.cc
    dumpfile.cc
    env.cc
    env_yandex.cc
    filename.cc
    filter_block.cc
    filter_policy.cc
    format.cc
    hash.cc
    histogram.cc
    iterator.cc
    log_reader.cc
    log_writer.cc
    logging.cc
    memenv.cc
    memtable.cc
    merger.cc
    options.cc
    port_yandex.cc
    repair.cc
    sequence.cc
    status.cc
    table.cc
    table_builder.cc
    table_cache.cc
    two_level_iterator.cc
    version_edit.cc
    version_set.cc
    write_batch.cc
)

END()

RECURSE(
    test
    test-perf
)
