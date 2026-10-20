#pragma once

#include "execution.h"

#include <ydb/public/api/protos/draft/fq.pb.h>

namespace NFq::NWasmServices {

// Adapter for FQ control-plane connections. The execution API is engine-neutral.
NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactory(
    const NConfig::TWasmServicesConfig& config, const THashMap<TString, FederatedQuery::Connection>& connections);
TString PrepareServiceConnection(const FederatedQuery::Connection& connection, const TString& currentIamToken);
void RegisterServiceTransforms(NYql::NDq::TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config,
    std::shared_ptr<ITvmTicketProvider> tvmProvider, ui32 userPool, NMonitoring::TDynamicCounterPtr counters = {});

}   // namespace NFq::NWasmServices
