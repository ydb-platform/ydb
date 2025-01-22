LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/testlib/pg
    ydb/core/tx/replication/ydb_proxy
    ydb/library/actors/core
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    mock_service.cpp
    test_env.h
    test_table.cpp
    write_topic.h
)

YQL_LAST_ABI_VERSION()

END()
