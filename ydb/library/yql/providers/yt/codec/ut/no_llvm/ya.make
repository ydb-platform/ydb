UNITTEST_FOR(ydb/library/yql/providers/yt/codec)

TAG(ya:manual)

SRCDIR(
    ydb/library/yql/providers/yt/codec/ut
)

SRCS(
    yt_codec_io_ut.cpp
)

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/minikql/codegen/no_llvm
    ydb/library/yql/minikql/computation/no_llvm
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/yt/lib/yson_helpers
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
