#pragma once

#include <ydb/core/fq/libs/config/protos/wasm_services.pb.h>
#include <ydb/core/fq/libs/wasm_services/transport.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/function/gateway/dq_function_gateway.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq::NWasmServices {

inline constexpr TStringBuf ServiceTransformType = "WasmService";

// Only public connection settings may enter the query plan. Credentials are
// resolved by the owning query engine and delivered through SecureParams.
struct TResolvedServiceConnection {
    TString Id;
    TString Settings;
};

using TServiceConnectionResolver = std::function<NThreading::TFuture<TResolvedServiceConnection>(const TString&)>;
using TServiceBindingResolver = std::function<TBinding(const TString&, const THashMap<TString, TString>&)>;

TString ServiceConnectionKey(const TString& id);
TString ServiceInvocationConnection(const TString& invocation);
TString ServiceInvocationSettings(const TString& invocation);

NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactoryWithResolver(
    const NConfig::TWasmServicesConfig& config, TServiceConnectionResolver resolver);

// Register once per engine/node, not once per query: every registered transform
// shares this executor and its operation/byte reservations.
void RegisterServiceTransformsWithResolver(NYql::NDq::TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config,
    TServiceBindingResolver resolver, ui32 userPool, NMonitoring::TDynamicCounterPtr counters = {});

}   // namespace NFq::NWasmServices
