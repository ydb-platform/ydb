LIBRARY()

SRCS(
    yql_yt_profiling.cpp
)

PEERDIR(
    ydb/library/yql/utils/log
    ydb/library/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

END()
