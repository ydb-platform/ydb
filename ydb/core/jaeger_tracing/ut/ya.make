UNITTEST_FOR(ydb/core/jaeger_tracing)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    sampler_ut.cpp
    throttler_ut.cpp
)

END()
