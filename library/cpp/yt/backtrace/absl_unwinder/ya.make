LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    absl_unwinder.cpp
)

PEERDIR(
    library/cpp/yt/backtrace/cursors/libunwind
    contrib/libs/tcmalloc/malloc_extension
)

END()
