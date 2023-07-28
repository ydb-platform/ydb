LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    interop.cpp
)

PEERDIR(
    library/cpp/yt/backtrace/cursors/frame_pointer
    contrib/libs/libunwind
)

END()
