LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    introspect.cpp
)
IF (OS_LINUX)
    SRCS(introspect_linux.cpp)
ELSE()
    SRCS(introspect_dummy.cpp)
ENDIF()

PEERDIR(
    yt/yt/core

    library/cpp/yt/backtrace/cursors/interop
    library/cpp/yt/backtrace/cursors/libunwind
    library/cpp/yt/backtrace/cursors/frame_pointer
    library/cpp/yt/misc
)

END()

RECURSE(
    http
)

RECURSE_FOR_TESTS(
    unittests
)
