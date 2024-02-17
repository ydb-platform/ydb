LIBRARY()

SRCS(
    symbolizer.cpp
    backtrace.cpp
)

PEERDIR(
    contrib/libs/backtrace
    library/cpp/deprecated/atomic
)


IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        backtrace_linux.cpp
    )

    PEERDIR(contrib/libs/libunwind)
    ADDINCL(contrib/libs/libunwind/include)
    
ELSE()
    SRCS(
        backtrace_dummy.cpp
    )
ENDIF()

END()