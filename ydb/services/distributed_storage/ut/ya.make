UNITTEST_FOR(ydb/services/distributed_storage)

SIZE(MEDIUM)

SRCS(
    distributed_storage_ut.cpp
)

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/formats
    ydb/core/protos
    ydb/core/testlib/default
    ydb/public/api/grpc/draft
)

YQL_LAST_ABI_VERSION()

END()
