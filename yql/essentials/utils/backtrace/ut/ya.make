UNITTEST_FOR(yql/essentials/utils/backtrace)

ENABLE(YQL_STYLE_CPP)

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        backtrace_ut.cpp
    )
ENDIF()

END()
