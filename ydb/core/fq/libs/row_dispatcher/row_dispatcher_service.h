#pragma once

#include <ydb/core/fq/libs/row_dispatcher/common/row_dispatcher_settings.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

#include <memory>

namespace NActors {

class TMon;

} // namespace NActors

namespace NFq {

std::unique_ptr<NActors::IActor> NewRowDispatcherService(
    const TRowDispatcherSettings& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway,
    NYdb::TDriver driver,
    NActors::TMon* monitoring = nullptr,
    ::NMonitoring::TDynamicCounterPtr countersRoot = MakeIntrusive<::NMonitoring::TDynamicCounters>(),
    NActors::TActorId nodesManagerId = {});

} // namespace NFq
