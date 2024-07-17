LIBRARY()

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/providers/yt/comp_nodes
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/common/codec
    ydb/library/yql/utils/failure_injector
    ydb/library/yql/parser/pg_wrapper/interface
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/common
    library/cpp/yson/node
    yt/yt/core
    ydb/library/yql/public/udf/arrow
    ydb/library/yql/public/udf
    contrib/libs/apache/arrow
    contrib/libs/flatbuffers
)

ADDINCL(
    contrib/libs/flatbuffers/include
)

IF(LINUX)
    PEERDIR(
        yt/yt/client
        yt/yt/client/arrow
        ydb/library/yql/providers/yt/lib/yt_rpc_helpers
    )

    SRCS(
        arrow_converter.cpp
        stream_decoder.cpp
        dq_yt_rpc_reader.cpp
        dq_yt_rpc_helpers.cpp
        dq_yt_block_reader.cpp
    )
    CFLAGS(
        -Wno-unused-parameter
    )
ENDIF()

SRCS(
    dq_yt_reader.cpp
    dq_yt_factory.cpp
    dq_yt_writer.cpp
)

YQL_LAST_ABI_VERSION()


END()
