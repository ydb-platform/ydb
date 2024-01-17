GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

SRCS(
    sensor_ut.cpp
    name_conflicts_ut.cpp
    profiler_ut.cpp
    solomon_ut.cpp
    tag_ut.cpp
    cube_ut.cpp
    exporter_ut.cpp
    perf_counter_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/library/profiling
    yt/yt/library/profiling/solomon
    yt/yt/library/profiling/tcmalloc
    yt/yt/library/profiling/resource_tracker
    yt/yt/library/profiling/perf
)

SIZE(MEDIUM)

END()

RECURSE(
    deps
)
