UNITTEST()

FORK_SUBTESTS()
SIZE(SMALL)

PEERDIR(
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/bridge/proxy
)

YQL_LAST_ABI_VERSION()

SRCS(
    get_min_iops_ut.cpp
    ${ARCADIA_ROOT}/ydb/core/testlib/actor_helpers.cpp
)

END()
