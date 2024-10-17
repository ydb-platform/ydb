LIBRARY()

SRCS(
    ut_helpers.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/providers/common/ut_helpers
    ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
