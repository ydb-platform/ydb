LIBRARY()

SRCS(
    yt_codec_io.cpp
    yt_codec_io.h
    yt_codec_job.cpp
    yt_codec_job.h
    yt_codec.cpp
    yt_codec.h
)

PEERDIR(
    library/cpp/streams/brotli
    library/cpp/yson
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/io
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/yql/providers/common/schema/parser
    ydb/library/yql/providers/yt/common
    ydb/library/yql/providers/yt/lib/mkql_helpers
    ydb/library/yql/providers/yt/lib/skiff
)

IF (MKQL_DISABLE_CODEGEN)
    CFLAGS(
        -DMKQL_DISABLE_CODEGEN
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    codegen
)

RECURSE_FOR_TESTS(
    ut
    ut/no_llvm
)
