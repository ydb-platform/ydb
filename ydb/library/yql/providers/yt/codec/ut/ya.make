UNITTEST_FOR(ydb/library/yql/providers/yt/codec)

TAG(ya:manual)

SRCS(
    yt_codec_io_ut.cpp
)

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/yt/lib/yson_helpers
    ydb/library/yql/providers/yt/codec/codegen
)

YQL_LAST_ABI_VERSION()

END()
