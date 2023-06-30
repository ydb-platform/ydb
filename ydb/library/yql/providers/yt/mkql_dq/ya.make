LIBRARY()

SRCS(
    yql_yt_dq_transform.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
