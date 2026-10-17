GTEST()

SRCS(
    provider_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/codec/codegen/llvm16
    yql/essentials/minikql/computation/llvm16
    yt/yql/providers/yt/gateway/lib
    yql/essentials/sql/pg
    ydb/library/yql/providers/yt/provider
    yql/essentials/core
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()
