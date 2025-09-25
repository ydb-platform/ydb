PROGRAM(join_perf)

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
    library/cpp/getopt
    library/cpp/getopt/small
    library/cpp/json
)

SRCS(
    main.cpp
    construct_join_graph.cpp
    joins.cpp
    benchmark_settings.cpp
)

END()

