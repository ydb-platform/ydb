LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    allocation_tag_profiler.cpp
)

PEERDIR(
    yt/yt/library/profiling
    yt/yt/library/ytprof
    yt/yt/core
)

END()


