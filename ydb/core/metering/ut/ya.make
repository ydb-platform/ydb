UNITTEST_FOR(ydb/core/metering)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRCS(
    stream_ru_calculator_ut.cpp
    time_grid_ut.cpp
)

END()
