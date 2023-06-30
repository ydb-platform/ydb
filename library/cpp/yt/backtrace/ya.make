LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    backtrace.cpp
)

IF (OS_WINDOWS)
    SRCS(
        symbolizers/dummy/dummy_symbolizer.cpp
    )
ELSE()
    SRCS(
        symbolizers/dynload/dynload_symbolizer.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/yt/string
)

END()

RECURSE(
    cursors/dummy
    cursors/frame_pointer
)

IF (NOT OS_WINDOWS)
    RECURSE(
        cursors/libunwind
    )
ENDIF()

IF (OS_LINUX)
    RECURSE(
        symbolizers/dwarf
    )

    RECURSE_FOR_TESTS(
        unittests
    )
ENDIF()
