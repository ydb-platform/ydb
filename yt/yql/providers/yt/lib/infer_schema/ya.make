LIBRARY()

SRCS(
    infer_schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yql/essentials/public/issue
    yql/essentials/utils/log
    yql/essentials/core/issue
)

IF(LINUX)
    PEERDIR(
        yt/yt/client
        yt/yql/providers/yt/lib/yt_rpc_helpers
    )

    SRCS(
        infer_schema_rpc.cpp
    )
ELSE()
    SRCS(
        infer_schema_rpc_impl.cpp
    )
ENDIF()

END()
