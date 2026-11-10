PROGRAM(ydb_query_replay)

IF (OS_LINUX)
    # Compare mold with LLD without a GDB index on the same inputs.
    SET(_LINKER_ID mold)
    LDFLAGS(
        -Wl,--no-gdb-index
        -Wl,--no-rosegment
        -Wl,--build-id=sha1
        -Wl,--perf
    )
    # Preserve the non-PIE flag normally added only for LLD.
    IF (NOT PIE)
        IF (NOT PIC OR PIC_NO_PIE)
            LDFLAGS(-Wl,-no-pie)
        ENDIF()
    ENDIF()
ENDIF()

ALLOCATOR(LF)

YQL_LAST_ABI_VERSION()
INCLUDE(${ARCADIA_ROOT}/ydb/tools/query_replay/common_deps.inc)

SRCS(${YDB_REPLAY_SRCS})

PEERDIR(${YDB_REPLAY_PEERDIRS})

END()
