LIBRARY()

SRCS(
    backtrace.cpp
    symbolize.cpp
)
IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        backtrace_in_context.cpp
        symbolizer_linux.cpp
    )
    PEERDIR(
        contrib/libs/libunwind
    )
ELSE()
    SRCS(
        symbolizer_dummy.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/deprecated/atomic
    library/cpp/dwarf_backtrace
)

END()

RECURSE_FOR_TESTS(
    ut
)

