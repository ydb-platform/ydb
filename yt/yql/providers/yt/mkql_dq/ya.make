LIBRARY()

SRCS(
    yql_yt_dq_transform.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
