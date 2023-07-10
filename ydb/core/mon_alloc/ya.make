LIBRARY()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(
        -DPROFILE_MEMORY_ALLOCATIONS
    )
ENDIF()

SRCS(
    monitor.cpp
    profiler.cpp
    stats.cpp
    tcmalloc.cpp
)

PEERDIR(
    contrib/libs/tcmalloc/malloc_extension
    library/cpp/actors/core
    library/cpp/actors/prof
    library/cpp/html/pcdata
    library/cpp/lfalloc/alloc_profiler
    library/cpp/lfalloc/dbg_info
    library/cpp/malloc/api
    library/cpp/monlib/service/pages
    library/cpp/ytalloc/api
    library/cpp/yt/memory
    ydb/core/base
    ydb/core/control
    ydb/library/services
)

END()
