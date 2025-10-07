LIBRARY()

YQL_ABI_VERSION(
    2
    28
    0
)

ENABLE(YQL_STYLE_CPP)

SRCS(
    static_udf.cpp
    topfreq.cpp
)

PEERDIR(
    yql/essentials/public/udf
)

END()
