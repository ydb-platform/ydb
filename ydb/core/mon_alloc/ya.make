LIBRARY()

OWNER(g:kikimr)

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(
        -DPROFILE_MEMORY_ALLOCATIONS
    )
ENDIF()

SRCS(
    monitor.cpp
    profiler.cpp
    stats.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/prof
    library/cpp/html/pcdata
    library/cpp/lfalloc/alloc_profiler 
    library/cpp/lfalloc/dbg_info 
    library/cpp/malloc/api
    library/cpp/monlib/service/pages
    library/cpp/ytalloc/api
    ydb/core/base
    ydb/core/control
)

IF (ALLOCATOR == "TCMALLOC_256K")
    SRCS(
        tcmalloc.cpp
    )
    PEERDIR(
        contrib/libs/tcmalloc
    )
ELSE()
    SRCS(
        tcmalloc_null.cpp
    )
ENDIF()

END()
