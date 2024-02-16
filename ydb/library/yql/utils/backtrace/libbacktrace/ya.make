LIBRARY()

SRCS(
    symbolizer.cpp
    backtrace.cpp
)

PEERDIR(
    contrib/libs/libunwind
    library/cpp/dwarf_backtrace
    library/cpp/deprecated/atomic
)

ADDINCL(
    contrib/libs/libunwind/include
)

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        backtrace_linux.cpp
    )
ELSE()
    SRCS(
        backtrace_dummy.cpp
    )
ENDIF()

END()