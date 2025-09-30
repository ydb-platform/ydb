#pragma once

#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/core/fq/libs/row_dispatcher/actors_factory.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NActors {
    class TMon;
}

namespace NKikimrConfig {
class TSharedReadingConfig;
} // namespace NKikimrConfig

namespace NFq {

std::unique_ptr<NActors::IActor> NewRowDispatcher(
    const NKikimrConfig::TSharedReadingConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const NFq::NRowDispatcher::IActorFactory::TPtr& actorFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const ::NMonitoring::TDynamicCounterPtr& countersRoot,
    const NYql::IPqGateway::TPtr& pqGateway,
    NYdb::TDriver driver,
    NActors::TMon* monitoring = nullptr);

} // namespace NFq
