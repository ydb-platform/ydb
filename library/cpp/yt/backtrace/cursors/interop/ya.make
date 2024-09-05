LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (ARCH_X86_64)
    SRCS(
        interop_x86_64.cpp
    )
ELSE()
    SRCS(
        interop_dummy.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/yt/backtrace/cursors/frame_pointer
    contrib/libs/libunwind
)

END()
