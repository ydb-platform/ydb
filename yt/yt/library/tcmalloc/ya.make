LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    tcmalloc_manager.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/oom
    yt/yt/library/profiling/resource_tracker
    contrib/libs/tcmalloc/malloc_extension
)

END()
