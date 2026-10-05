LIBRARY()
SRCS(yql_qyt_read_session.cpp yql_qyt_message_stream_client.cpp yql_yt_client.cpp)
PEERDIR(
    ydb/library/yql/providers/abstract/message_stream
    yt/yt/client
)
YQL_LAST_ABI_VERSION()
END()
RECURSE_FOR_TESTS(ut)
