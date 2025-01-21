UNITTEST()

SRCS(
    yql_yt_native_folders_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm14
    yt/yql/providers/yt/lib/ut_common
    library/cpp/testing/mock_server
    library/cpp/testing/common
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/minikql/invoke_builtins/llvm14
)

YQL_LAST_ABI_VERSION()

END()

