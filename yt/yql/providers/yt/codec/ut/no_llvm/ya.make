UNITTEST_FOR(yt/yql/providers/yt/codec)

SRCDIR(
    yt/yql/providers/yt/codec/ut
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
    yt/yql/providers/yt/lib/yson_helpers
    yt/yql/providers/yt/codec/codegen/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
