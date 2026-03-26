UNITTEST()

SRCS(
    yql_yt_coordinator_ut.cpp
    yql_yt_gateway_coordinator_integration_ut.cpp
    yql_yt_ordered_partitioner_ut.cpp
    yql_yt_fmr_partitioner_ut.cpp
    yql_yt_sorted_partitioner_ut.cpp
    yql_yt_fmr_boundary_keys_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/job_preparer/impl
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/fmr/test_tools/fmr_coordinator_service_helper
    yt/yql/providers/yt/fmr/test_tools/fmr_gateway_helpers
    yt/yql/providers/yt/fmr/test_tools/mock_time_provider
    yt/yql/providers/yt/fmr/test_tools/sorted_partitioner
    yt/yql/providers/yt/fmr/test_tools/table_data_service
    yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl
    yt/yql/providers/yt/gateway/fmr
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/codec/codegen/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/core/file_storage/proto
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg
    yql/essentials/utils/failure_injector
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
