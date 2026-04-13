LIBRARY()

SRCS(
    kqp_script_execution_compression.cpp
    kqp_script_execution_retries.cpp
)

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/protobuf/interop
    ydb/core/protos
    ydb/core/tx/datashard
    ydb/library/yverify_stream
    ydb/public/api/protos
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
