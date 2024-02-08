#pragma once

#include <ydb/core/base/appdata.h>

#include <ydb/core/fq/libs/actors/nodes_manager.h>
#include <ydb/core/fq/libs/actors/proxy.h>
#include <ydb/core/fq/libs/actors/proxy_private.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/shared_resources/interface/shared_resources.h>

#include <ydb/library/folder_service/proto/config.pb.h>
#include <ydb/core/fq/libs/config/protos/audit.pb.h>

#include <ydb/library/yql/providers/pq/cm_client/client.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>

namespace NFq {

using TActorRegistrator = std::function<void(NActors::TActorId, NActors::IActor*)>;

IYqSharedResources::TPtr CreateYqSharedResources(
    const NFq::NConfig::TConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters);

void Init(
    const NFq::NConfig::TConfig& config,
    ui32 nodeId,
    const TActorRegistrator& actorRegistrator,
    const NKikimr::TAppData* appData,
    const TString& tenant,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const IYqSharedResources::TPtr& yqSharedResources,
    const std::function<IActor*(const NKikimrProto::NFolderService::TFolderServiceConfig& authConfig)>& folderServiceFactory,
    ui32 icPort,
    const std::vector<NKikimr::NMiniKQL::TComputationNodeFactory>& additionalCompNodeFactories
);

} // NFq
