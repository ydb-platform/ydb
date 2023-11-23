GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    symbolizer_ut.cpp
    cpu_profiler_ut.cpp
    heap_profiler_ut.cpp
    spinlock_profiler_ut.cpp
    queue_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/ytprof
    yt/yt/library/profiling
    yt/yt/core
)

IF (OS_LINUX)
    LDFLAGS("-Wl,--build-id=sha1")
ENDIF()

DEPENDS(
    yt/yt/library/ytprof/unittests/testso
    yt/yt/library/ytprof/unittests/testso1
)

IF (BUILD_TYPE != "release" AND BUILD_TYPE != "relwithdebinfo")
    CFLAGS(-DYTPROF_DEBUG_BUILD)
ENDIF()

SIZE(MEDIUM)

ALLOCATOR(TCMALLOC_256K)

END()

RECURSE(
    testso
    testso1
)
