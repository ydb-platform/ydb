LIBRARY()

PEERDIR(
    ydb/library/yql/minikql/computation/llvm
    ydb/library/yql/providers/yt/comp_nodes
    ydb/library/yql/providers/yt/codec
    ydb/library/yql/providers/common/codec
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/common
    library/cpp/yson/node
    yt/yt/core
)

IF(LINUX)
    PEERDIR(
        yt/yt/client
        yt/yt/client/arrow
    )

    SRCS(
        dq_yt_rpc_reader.cpp
    )
ENDIF()

SRCS(
    dq_yt_reader.cpp
    dq_yt_factory.cpp
    dq_yt_writer.cpp
)


YQL_LAST_ABI_VERSION()


END()
