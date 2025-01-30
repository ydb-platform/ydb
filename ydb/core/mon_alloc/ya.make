LIBRARY()

SRCS(
    memory_info.cpp
    monitor.cpp
    profiler.cpp
    stats.cpp
    tcmalloc.cpp
)

IF (OS_LINUX AND ARCH_X86_64)
    CFLAGS(
        -DUSE_DWARF_BACKTRACE
    )
    PEERDIR(
        library/cpp/dwarf_backtrace
    )
ENDIF()

PEERDIR(
    contrib/libs/tcmalloc/malloc_extension
    library/cpp/html/pcdata
    library/cpp/lfalloc/alloc_profiler
    library/cpp/lfalloc/dbg_info
    library/cpp/malloc/api
    library/cpp/monlib/service/pages
    ydb/core/base
    ydb/core/control
    ydb/library/actors/core
    ydb/library/actors/prof
    ydb/library/services
    yql/essentials/utils/memory_profiling
)

END()
