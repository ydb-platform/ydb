LIBRARY()

SRCS(
    backtrace.cpp
    backtrace_lib.cpp
    symbolize.cpp
    symbolizer.cpp
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

PEERDIR_FROM_POLICY(
    ALLOW
    ads/core/tools/unified_agent
    logbroker/unified_agent
    sdg/infra/sdc-unified-agent
    alice/rtlog/agent
    robot/rthub
)

END()

RECURSE_FOR_TESTS(ut)

