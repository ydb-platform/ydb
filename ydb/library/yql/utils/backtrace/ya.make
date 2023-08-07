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
        ydb/library/yql/utils/backtrace/fake_llvm_symbolizer
        contrib/libs/libunwind
    )
ELSE()
    SRCS(
        symbolizer_dummy.cpp
    )
    PEERDIR(
        ydb/library/yql/utils/backtrace/fake_llvm_symbolizer
    )
ENDIF()

PEERDIR(
    contrib/libs/llvm12/lib/DebugInfo/Symbolize
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)

