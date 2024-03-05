LIBRARY()

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    ip_base_udf.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
    library/cpp/ipmath
    library/cpp/ipv6_address
)

END()
