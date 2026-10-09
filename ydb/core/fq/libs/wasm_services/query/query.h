#pragma once

#include <ydb/core/fq/libs/config/protos/wasm_services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/function/gateway/dq_function_gateway.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NFq::NWasmServices {

NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactory(
    const NConfig::TWasmServicesConfig& config, const THashMap<TString, FederatedQuery::Connection>& connections);
TString ServiceConnectionKey(const TString& id);
TString ServiceInvocationConnection(const TString& invocation);
TString PrepareServiceConnection(const FederatedQuery::Connection& connection, const TString& currentIamToken);
void RegisterServiceTransforms(NYql::NDq::TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config);

} // namespace NFq::NWasmServices
