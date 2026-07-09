GTEST()

SRCS(
    yt_client_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/public/api/protos
    contrib/libs/apache/arrow
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/client
)

SIZE(SMALL)

END()
