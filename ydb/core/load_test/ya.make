LIBRARY()

SRCS(
    aggregated_result.cpp
    archive.cpp
    config_examples.cpp
    service_actor.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/writer
    library/cpp/monlib/service/pages
    library/cpp/time_provider
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/load_test/blobstorage
    ydb/core/load_test/common
    ydb/core/load_test/ddisk
    ydb/core/load_test/interconnect
    ydb/core/load_test/keyvalue
    ydb/core/load_test/kqp
    ydb/core/load_test/nbs
    ydb/core/load_test/ycsb
    ydb/core/protos
    ydb/library/actors/interconnect
    ydb/library/mkql_proto/protos
    ydb/public/lib/base
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/library/operation_id
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_ycsb
)
