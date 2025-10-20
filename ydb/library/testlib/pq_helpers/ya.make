LIBRARY()

SRCS(
    mock_pq_gateway.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/testlib/actors
    ydb/library/testlib/common
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/library/yql/providers/pq/provider
)

YQL_LAST_ABI_VERSION()

END()
