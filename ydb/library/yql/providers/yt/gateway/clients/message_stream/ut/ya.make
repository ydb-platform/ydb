UNITTEST_FOR(ydb/library/yql/providers/yt/gateway/clients/message_stream)

SIZE(MEDIUM)

SRCS(
    yql_qyt_blocking_queue_ut.cpp
    yql_qyt_message_stream_client_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/abstract/message_stream
    ydb/library/yql/providers/yt/gateway/clients/message_stream
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    message_stream
)
