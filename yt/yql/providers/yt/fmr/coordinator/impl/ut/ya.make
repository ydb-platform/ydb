UNITTEST()

SRCS(
    yql_yt_coordinator_ut.cpp
    yql_yt_gateway_coordinator_integration_ut.cpp
    yql_yt_partitioner_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/fmr/process
    yt/yql/providers/yt/fmr/test_tools/fmr_gateway_helpers
    yt/yql/providers/yt/fmr/test_tools/mock_time_provider
    yt/yql/providers/yt/gateway/fmr
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/codec/codegen/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg
    yql/essentials/utils/failure_injector
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
