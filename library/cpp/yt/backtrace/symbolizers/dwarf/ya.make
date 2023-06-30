LIBRARY()

SRCS(
    GLOBAL dwarf_symbolizer.cpp
)

PEERDIR(
    library/cpp/dwarf_backtrace
    library/cpp/yt/backtrace
)

END()

IF (BUILD_TYPE == "DEBUG" OR BUILD_TYPE == "PROFILE")
    RECURSE_FOR_TESTS(
        unittests
    )
ENDIF()
