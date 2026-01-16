LIBRARY()

SRCS(
    mock_pq_gateway.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/testlib/actors
    ydb/library/testlib/common
    ydb/library/yql/providers/pq/gateway/abstract
)

YQL_LAST_ABI_VERSION()

END()
