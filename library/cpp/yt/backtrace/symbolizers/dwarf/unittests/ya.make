GTEST()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    backtrace_ut.cpp
)

CXXFLAGS(
    -g
)

IF (BUILD_TYPE == "DEBUG" OR BUILD_TYPE == "PROFILE")
    CXXFLAGS(-DSYMBOLIZED_BUILD)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    library/cpp/testing/gtest
    library/cpp/yt/backtrace/symbolizers/dwarf
    library/cpp/yt/backtrace/cursors/libunwind
)

END()
