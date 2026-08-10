UNITTEST_FOR(ydb/core/jaeger_tracing)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

PEERDIR(
    library/cpp/random_provider
    ydb/library/actors/wilson
)

SRCS(
    sampler_ut.cpp
    sampling_throttling_control_ut.cpp
    throttler_ut.cpp
)

END()
