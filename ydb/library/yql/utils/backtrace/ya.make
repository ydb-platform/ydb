LIBRARY()

SRCS(
    backtrace.cpp
    backtrace_lib.cpp
    symbolize.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
)


IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        backtrace_linux.cpp
        symbolizer_linux.cpp
    )

    PEERDIR(
        contrib/libs/backtrace
        contrib/libs/libunwind
    )
    ADDINCL(contrib/libs/libunwind/include)
    
ELSE()
    SRCS(
        backtrace_dummy.cpp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(ut)