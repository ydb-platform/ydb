LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    signal_safe_profiler.cpp
    cpu_profiler.cpp
    heap_profiler.cpp
    spinlock_profiler.cpp
    profile.cpp
    build_info.cpp
    external_pprof.cpp
)

IF (OS_LINUX)
    SRCS(symbolize.cpp)
ELSE()
    SRCS(symbolize_other.cpp)
ENDIF()

PEERDIR(
    library/cpp/yt/memory
    library/cpp/yt/threading
    library/cpp/yt/backtrace/cursors/interop
    library/cpp/yt/backtrace/cursors/frame_pointer
    library/cpp/yt/backtrace/cursors/libunwind
    yt/yt/library/ytprof/api
    yt/yt/library/ytprof/proto
    contrib/libs/libunwind
    contrib/libs/tcmalloc/malloc_extension
    library/cpp/svnversion
    yt/yt/core
)

IF (NOT OPENSOURCE)
    PEERDIR(
        yt/yt/library/ytprof/bundle
    )
ENDIF()

IF (OS_SDK == "local")
    CXXFLAGS(-DYT_NO_AUXV)
ENDIF()

IF (BUILD_TYPE == "PROFILE")
    CXXFLAGS(-DYTPROF_PROFILE_BUILD)
ENDIF()

CXXFLAGS(-DYTPROF_BUILD_TYPE='\"${BUILD_TYPE}\"')

END()

RECURSE(
    allocation_tag_profiler
    http
    example
    bundle
)

IF (OS_LINUX)
    RECURSE(
        integration
    )

    IF (NOT OPENSOURCE)
        RECURSE(
            benchmark
        )
    ENDIF()

    IF (NOT SANITIZER_TYPE AND NOT YT_TEAMCITY)
        RECURSE(unittests)
    ENDIF()
ENDIF()

