LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (ARCH_X86_64)
    SRCS(
        frame_pointer_cursor_x86_64.cpp
    )
ELSE()
    SRCS(
        frame_pointer_cursor_dummy.cpp
    )
ENDIF()

END()
