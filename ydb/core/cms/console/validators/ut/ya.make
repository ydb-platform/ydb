UNITTEST_FOR(ydb/core/cms/console/validators)

OWNER(ienkovich g:kikimr)

FORK_SUBTESTS()

TIMEOUT(600)
SIZE(MEDIUM)

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
