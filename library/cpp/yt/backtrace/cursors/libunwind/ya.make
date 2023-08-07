LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    libunwind_cursor.cpp
)

PEERDIR(
    contrib/libs/libunwind
)

END()
