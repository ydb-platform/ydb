LIBRARY()

SRCS(
    ../kqp.cpp
    ../yql_single_query.cpp
    ../yql_single_query.h
)

PEERDIR(
    library/cpp/histogram/hdr
    library/cpp/monlib/service/pages
    library/cpp/time_provider
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/kqp/common
    ydb/core/load_test/common
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/workload/abstract
    ydb/library/workload/kv
    ydb/library/workload/stock
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/library/operation_id
)

END()
