LIBRARY()

SRCS(
    ut_helpers.cpp
)

PEERDIR(
    ydb/library/testlib/common
    ydb/library/yql/providers/common/ut_helpers
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/public/sdk/cpp/src/client/datastreams
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/minikql/computation/llvm16
    yql/essentials/providers/common/proto
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
