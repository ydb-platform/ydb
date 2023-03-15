LIBRARY()

YQL_ABI_VERSION(
    2
    27
    0
)

SRCS(
    static_udf.cpp
    topfreq.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
)

END()
