LIBRARY()

SRCS(
    GLOBAL dwarf_symbolizer.cpp
)

PEERDIR(
    library/cpp/dwarf_backtrace
    library/cpp/yt/backtrace
)

END()

RECURSE_FOR_TESTS(
    unittests
)
