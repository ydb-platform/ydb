GTEST()

SIZE(MEDIUM)

SRCS(
    message_stream_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/qyt/gateway/clients/message_stream
    yt/yt/client/unittests/mock
)

YQL_LAST_ABI_VERSION()

END()
