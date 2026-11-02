YQL_LIBRARY()

SRCS(
    yql_yt_message_stream_source.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/common/message_stream/async_io
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/yt/gateway/clients/message_stream
    ydb/library/yql/providers/yt/proto
)

END()
