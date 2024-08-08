UNITTEST_FOR(ydb/core/tablet_flat)

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    util_pool_ut.cpp
    util_string_ut.cpp
)

END()
