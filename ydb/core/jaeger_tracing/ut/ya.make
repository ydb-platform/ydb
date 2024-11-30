UNITTEST_FOR(ydb/core/jaeger_tracing)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    sampler_ut.cpp
    throttler_ut.cpp
)

END()
