#pragma once

#include <ydb/core/fq/libs/config/protos/wasm_services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/providers/function/gateway/dq_function_gateway.h>

namespace NFq::NWasmServices {

NYql::TDqFunctionGatewayFactory::TPtr CreateServiceGatewayFactory(const NConfig::TWasmServicesConfig& config);
void RegisterServiceTransforms(NYql::NDq::TDqAsyncIoFactory& factory, const NConfig::TWasmServicesConfig& config);

} // namespace NFq::NWasmServices
