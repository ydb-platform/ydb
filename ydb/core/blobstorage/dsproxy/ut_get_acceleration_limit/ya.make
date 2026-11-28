UNITTEST()

FORK_SUBTESTS()
SIZE(SMALL)

PEERDIR(
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/bridge/proxy
    ydb/core/control/lib
    ydb/core/util/actorsys_test
)

YQL_LAST_ABI_VERSION()

SRCS(
    get_acceleration_limit_ut.cpp
)

END()
