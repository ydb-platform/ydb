#pragma once

#include <ydb/core/fq/libs/config/protos/row_dispatcher.pb.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include "events/data_plane.h"

#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NActors {
    class TMon;
}

namespace NFq {

std::unique_ptr<NActors::IActor> NewRowDispatcherService(
    const NConfig::TRowDispatcherConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const TString& tenant,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    const NYql::IPqGateway::TPtr& pqGateway,
    NActors::TMon* monitoring = nullptr,
    ::NMonitoring::TDynamicCounterPtr countersRoot = MakeIntrusive<::NMonitoring::TDynamicCounters>());

} // namespace NFq
