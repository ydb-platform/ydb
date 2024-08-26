LIBRARY()

SRCS(
    ut_helpers.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/providers/common/ut_helpers
    ydb/public/sdk/cpp/client/ydb_datastreams
    ydb/public/sdk/cpp/client/ydb_persqueue_public
)

YQL_LAST_ABI_VERSION()

END()
