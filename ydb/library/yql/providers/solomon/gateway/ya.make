LIBRARY()

OWNER(
    monster
    g:yql
)

SRCS(
    yql_solomon_gateway.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/providers/solomon/provider
)

END()
