LIBRARY()

SRCS(
    mock_pq_gateway.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/testlib/actors
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/library/yql/providers/pq/provider
)

YQL_LAST_ABI_VERSION()

END()
