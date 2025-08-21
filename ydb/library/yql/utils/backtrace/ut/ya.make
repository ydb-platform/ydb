UNITTEST_FOR(ydb/library/yql/utils/backtrace)

TAG(ya:manual)


IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        backtrace_ut.cpp
    )
ENDIF()

END()
