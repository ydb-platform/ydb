UNITTEST_FOR(ydb/core/jaeger_tracing)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

SRCS(
    sampler_ut.cpp
    throttler_ut.cpp
)

END()
