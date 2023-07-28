GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/backtrace
    library/cpp/yt/backtrace/cursors/interop
    library/cpp/yt/backtrace/cursors/frame_pointer
    library/cpp/yt/backtrace/cursors/libunwind
    library/cpp/yt/memory
)

IF (BUILD_TYPE == "DEBUG" OR BUILD_TYPE == "PROFILE")
    SRCS(
        backtrace_ut.cpp
    )
ENDIF()

END()
