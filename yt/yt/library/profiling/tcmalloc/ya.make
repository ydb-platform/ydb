LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    profiler.cpp
)

PEERDIR(
    yt/yt/library/profiling
    contrib/libs/tcmalloc/malloc_extension
)

END()
