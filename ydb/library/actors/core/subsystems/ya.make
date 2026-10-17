LIBRARY()

NO_WSHADOW()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

IF (ALLOCATOR == "B" OR ALLOCATOR == "BS" OR ALLOCATOR == "C")
    CXXFLAGS(-DBALLOC)
    PEERDIR(
        library/cpp/balloc/optional
    )
ENDIF()

SRCS(
    allocation_cache.cpp  # gn: into ydb/library/actors/core
    allocation_cache.h
    allocation_cache_tls.h
    async_frame_cache.h
    inmemory_metrics.cpp  # gn: into ydb/library/actors/core
    metric_system.cpp  # gn: into ydb/library/actors/core
    stats.cpp  # gn: into ydb/library/actors/core
)

PEERDIR(
    library/cpp/lwtrace
    ydb/library/actors/metrics
    ydb/library/actors/util
    ydb/library/actors/protos
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        ../tsan.supp
    )
ENDIF()

END()
