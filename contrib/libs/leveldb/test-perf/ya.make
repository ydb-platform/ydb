PROGRAM(leveldb-perf)

NO_COMPILER_WARNINGS()

ALLOCATOR(LF)

PEERDIR(
    contrib/libs/leveldb
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
    db_bench.cc
    testutil.cc
)

END()
