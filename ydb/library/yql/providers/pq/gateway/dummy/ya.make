LIBRARY()

SRCS(
    yql_pq_dummy_gateway_factory.cpp
    yql_pq_dummy_gateway.cpp
)

PEERDIR(
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/gateway/clients/file
)

END()
