LIBRARY()

SRCS(
    yt_arrow_converter.cpp
    yt_arrow_output_converter.cpp
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
    contrib/libs/apache/arrow
    yql/essentials/core
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/result_format
    yql/essentials/public/udf/arrow
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/codec/arrow
    yql/essentials/providers/common/schema/mkql
    yql/essentials/providers/common/schema/parser
    yt/yql/providers/yt/common
    yt/yql/providers/yt/lib/mkql_helpers
    yt/yql/providers/yt/lib/skiff
    yql/essentials/providers/common/codec/yt_arrow_converter_interface
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
