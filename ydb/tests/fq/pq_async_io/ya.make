LIBRARY()

SRCS(
    mock_pq_gateway.cpp
    ut_helpers.cpp
)

PEERDIR(
    yql/essentials/minikql/computation/llvm14
    ydb/library/yql/providers/common/ut_helpers
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
