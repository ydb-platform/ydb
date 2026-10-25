UNITTEST_FOR(ydb/core/jaeger_tracing)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (NOT SANITIZER_TYPE)
    REQUIREMENTS(cpu:1)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    sampler_ut.cpp
    throttler_ut.cpp
)

END()
