#pragma once

#include <ydb/core/fq/libs/wasm_services/query/execution.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr::NKqp {

void RegisterKqpWasmServiceProvider(NYql::TTypeAnnotationContext& types, const NFq::NConfig::TWasmServicesConfig& config,
    TIntrusivePtr<NYql::IKikimrGateway> gateway, const TString& cluster, NExternalSource::IExternalSourceFactory::TPtr externalSources);

void RegisterKqpWasmServiceTransforms(NYql::NDq::TDqAsyncIoFactory& factory, const NFq::NConfig::TWasmServicesConfig& config,
    std::shared_ptr<NFq::NWasmServices::ITvmTicketProvider> tvmProvider, ui32 userPool, NMonitoring::TDynamicCounterPtr counters);

}   // namespace NKikimr::NKqp
