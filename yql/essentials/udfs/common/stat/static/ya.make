LIBRARY()

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    static_udf.cpp
    stat_udf.h
)

PEERDIR(
    yql/essentials/public/udf
    library/cpp/tdigest
)

END()
