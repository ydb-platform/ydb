PROGRAM(ydb_query_replay)

IF (OS_LINUX)
    # Relink with a trace to compare the cost of generating the GDB index.
    ENABLE(LINKER_TIME_TRACE)
    LDFLAGS(-Wl,--gdb-index)
ENDIF()

ALLOCATOR(LF)

YQL_LAST_ABI_VERSION()
INCLUDE(${ARCADIA_ROOT}/ydb/tools/query_replay/common_deps.inc)

SRCS(${YDB_REPLAY_SRCS})

PEERDIR(${YDB_REPLAY_PEERDIRS})

END()
