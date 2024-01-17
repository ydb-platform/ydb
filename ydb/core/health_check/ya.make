LIBRARY()

SRCS(
    health_check.cpp
    health_check.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/library/aclib
    ydb/public/api/protos
    ydb/public/api/grpc
    ydb/library/yql/public/issue/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
