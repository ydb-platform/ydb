LIBRARY()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(
        -DPROFILE_MEMORY_ALLOCATIONS
    )
ENDIF()

SRCS(
    memory_info.cpp
    monitor.cpp
    profiler.cpp
    stats.cpp
    tcmalloc.cpp
)

PEERDIR(
    contrib/libs/tcmalloc/malloc_extension
    library/cpp/dwarf_backtrace
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
)

END()
