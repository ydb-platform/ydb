UNITTEST_FOR(ydb/library/yql/providers/yt/codec)

SRCDIR(
    ydb/library/yql/providers/yt/codec/ut
)

SRCS(
    yt_codec_io_ut.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/minikql/codegen/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/mkql
    ydb/library/yql/providers/yt/lib/yson_helpers
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
