LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/local_rpc
    ydb/core/protos
    ydb/core/testlib/pg
    ydb/core/tx/replication/ydb_proxy
    ydb/library/actors/core
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    mock_service.cpp
    test_env.h
    test_table.cpp
    test_topic.cpp
    write_topic.h
)

YQL_LAST_ABI_VERSION()

END()
