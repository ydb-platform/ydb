UNITTEST_FOR(ydb/core/driver_lib/version)

SRCS(version_ut.cpp)

TIMEOUT(300)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/driver_lib/version
    ydb/apps/version
)

END()
