LIBRARY()

SRCS(
    ut_helpers.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/providers/common/ut_helpers
    ydb/public/sdk/cpp/client/ydb_datastreams
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/library/yql/minikql/comp_nodes/llvm14
)

YQL_LAST_ABI_VERSION()

END()
