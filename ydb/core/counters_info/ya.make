LIBRARY()

SRCS(
    counters_info.cpp
    counters_info.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/mon
    ydb/library/aclib
    ydb/public/api/protos
    ydb/public/api/grpc
    yql/essentials/public/issue/protos
)

END()

