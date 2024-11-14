LIBRARY()

SRCS(
    yql_yt_mixed.cpp
)

PEERDIR(
    yql/essentials/utils/log
    ydb/library/yql/providers/yt/provider
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/providers/yt/gateway/native
    ydb/library/yql/providers/yt/gateway/lib
    ydb/library/yql/providers/yt/common
    yql/essentials/providers/common/provider

    library/cpp/threading/future
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()
