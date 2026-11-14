UNITTEST_FOR(ydb/core/driver_lib/actor_system_config)

FORK_SUBTESTS()

SIZE(SMALL)

SRCS(
    auto_config_initializer_ut.cpp
    config_helpers_ut.cpp
)

END()
