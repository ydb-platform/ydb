LIBRARY()

SRCS(
    yql_yt_fmr_gateway_helpers.cpp
)

PEERDIR(
    library/cpp/time_provider
    yql/essentials/core/file_storage
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins
    yt/yql/providers/yt/fmr/coordinator/interface
    yt/yql/providers/yt/fmr/fmr_tool_lib
    yt/yql/providers/yt/fmr/job_launcher
    yt/yql/providers/yt/fmr/test_tools/mock_time_provider
    yt/yql/providers/yt/fmr/test_tools/table_data_service
    yt/yql/providers/yt/fmr/yt_job_service/file
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/gateway/fmr
)

YQL_LAST_ABI_VERSION()

END()

