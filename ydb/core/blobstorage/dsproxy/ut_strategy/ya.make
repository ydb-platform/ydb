UNITTEST()

FORK_SUBTESTS()

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/blobstorage/dsproxy
)

YQL_LAST_ABI_VERSION()

SRCS(
    strategy_ut.cpp
)

END()
