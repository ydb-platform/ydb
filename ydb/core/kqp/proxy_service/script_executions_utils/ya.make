LIBRARY()

SRCS(
    kqp_script_execution_compression.cpp
    kqp_script_execution_retries.cpp
    kqp_script_execution_utils.cpp
)

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/json/writer
    ydb/core/kqp/common
    ydb/core/protos
    ydb/core/tx/datashard
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
