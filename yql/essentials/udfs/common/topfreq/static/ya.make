LIBRARY()

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    static_udf.cpp
    topfreq.cpp
)

PEERDIR(
    yql/essentials/public/udf
)

END()
