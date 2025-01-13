LIBRARY()

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/computation/llvm14
    yt/yql/providers/yt/comp_nodes
    yt/yql/providers/yt/codec
    yql/essentials/providers/common/codec
    yql/essentials/utils/failure_injector
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/common
    library/cpp/yson/node
    yt/yt/core
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf
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
        yt/yql/providers/yt/lib/yt_rpc_helpers
    )

    SRCS(
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
