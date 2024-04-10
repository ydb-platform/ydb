LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/tx/replication/ydb_proxy
    ydb/public/sdk/cpp/client/ydb_topic
    library/cpp/testing/unittest
)

SRCS(
    test_env.h
    test_table.cpp
    write_topic.h
)

YQL_LAST_ABI_VERSION()

END()
