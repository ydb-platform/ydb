UNITTEST()

FORK_SUBTESTS()

SPLIT_FACTOR(30)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

PEERDIR(
    ydb/library/actors/protos
    ydb/library/actors/util
    library/cpp/getopt
    library/cpp/svnversion
    ydb/apps/version
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/bridge/proxy
    ydb/core/blobstorage/dsproxy
    ydb/core/blobstorage/groupinfo
    ydb/core/blobstorage/pdisk
    ydb/core/blobstorage/vdisk
    ydb/core/blobstorage/vdisk/common
    ydb/core/mon_alloc
    yql/essentials/sql/pg_dummy
)

SRCS(
    dsproxy_ut.cpp
)

END()
