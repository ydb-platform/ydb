UNITTEST_FOR(ydb/library/keys)
FORK_SUBTESTS()

SIZE(SMALL)
REQUIREMENTS(cpu:1)

PEERDIR()

SRCS(
    default_keys_ut.cpp
)

END()
