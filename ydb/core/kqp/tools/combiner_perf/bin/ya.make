PROGRAM(combiner_perf)
ALLOCATOR(LF_DBG)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

PEERDIR(
    ydb/core/kqp/tools/combiner_perf
    library/cpp/lfalloc/alloc_profiler
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)

SRCS(
    main.cpp
)

END()
