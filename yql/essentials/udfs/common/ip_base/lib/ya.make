LIBRARY()

YQL_ABI_VERSION(
    2
    43
    0
)

SRCS(
    ip_base_udf.cpp
)

PEERDIR(
    yql/essentials/public/udf
    library/cpp/ipmath
    library/cpp/ipv6_address
)

END()
