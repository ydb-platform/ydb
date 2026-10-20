#pragma once

#include <ydb/core/fq/libs/config/protos/wasm_services.pb.h>
#include <ydb/core/fq/libs/wasm_services/tvm.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/function/gateway/dq_function_gateway.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq::NWasmServices {

inline constexpr TStringBuf ServiceTransformType = "WasmService";

NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactory(
    const NConfig::TWasmServicesConfig& config, const THashMap<TString, FederatedQuery::Connection>& connections);
TString ServiceConnectionKey(const TString& id);
TString ServiceInvocationConnection(const TString& invocation);
TString PrepareServiceConnection(const FederatedQuery::Connection& connection, const TString& currentIamToken);
void RegisterServiceTransforms(NYql::NDq::TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config,
    std::shared_ptr<ITvmTicketProvider> tvmProvider, ui32 userPool, NMonitoring::TDynamicCounterPtr counters = {});

}   // namespace NFq::NWasmServices
