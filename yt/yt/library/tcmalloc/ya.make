LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    tcmalloc_manager.cpp
    GLOBAL configure_tcmalloc_manager.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/ytprof
    yt/yt/library/profiling/resource_tracker
    contrib/libs/tcmalloc/malloc_extension
)

END()
