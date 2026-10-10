GTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    message_stream_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/yt/gateway/clients/message_stream
    yt/yt/client/unittests/mock
)

YQL_LAST_ABI_VERSION()

END()
