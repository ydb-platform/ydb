UNITTEST_FOR(ydb/core/cms/console/validators)

FORK_SUBTESTS()

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/testing/unittest
)

SRCS(
    registry_ut.cpp
    validator_bootstrap_ut.cpp
    validator_nameservice_ut.cpp
    validator_ut_common.h
)

END()
