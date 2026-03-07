UNITTEST_FOR(ydb/core/jaeger_tracing)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    sampler_ut.cpp
    throttler_ut.cpp
)

END()
