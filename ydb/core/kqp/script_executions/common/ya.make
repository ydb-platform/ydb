LIBRARY()

SRCS(
    kqp_script_execution_compression.cpp
    kqp_script_execution_retries.cpp
    kqp_script_executions.cpp
)

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/json/writer
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/protos
    ydb/core/tx/datashard
    ydb/library/aclib
    ydb/library/yverify_stream
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/library/operation_id/protos
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
