LIBRARY()

SRCS(
    ut_helpers.cpp
)

PEERDIR(
    yql/essentials/minikql/computation/llvm14
    ydb/library/yql/providers/common/ut_helpers
    ydb/public/sdk/cpp/client/ydb_topic
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
