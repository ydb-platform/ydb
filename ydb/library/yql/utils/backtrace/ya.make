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
    ADDINCL(
        contrib/libs/libunwind/include
    )
    PEERDIR(
        contrib/libs/libunwind
        library/cpp/dwarf_backtrace
    )
ELSE()
    SRCS(
        symbolizer_dummy.cpp
    )
ENDIF()

PEERDIR(
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)

