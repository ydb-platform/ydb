#pragma once

#include <ydb/core/base/appdata.h>

#include <ydb/core/yq/libs/actors/nodes_manager.h>
#include <ydb/core/yq/libs/actors/proxy.h>
#include <ydb/core/yq/libs/actors/proxy_private.h>
#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/shared_resources/interface/shared_resources.h>

#include <ydb/library/folder_service/proto/config.pb.h>
#include <ydb/core/yq/libs/config/protos/audit.pb.h>

#include <ydb/library/yql/providers/pq/cm_client/client.h>

#include <library/cpp/actors/core/actor.h>

#include <util/generic/ptr.h>

namespace NYq {

using TActorRegistrator = std::function<void(NActors::TActorId, NActors::IActor*)>;

IYqSharedResources::TPtr CreateYqSharedResources(
    const NYq::NConfig::TConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const NMonitoring::TDynamicCounterPtr& counters);

void Init(
    const NYq::NConfig::TConfig& config,
    ui32 nodeId,
    const TActorRegistrator& actorRegistrator,
    const NKikimr::TAppData* appData,
    const TString& tenant,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const IYqSharedResources::TPtr& yqSharedResources,
    const std::function<IActor*(const NKikimrProto::NFolderService::TFolderServiceConfig& authConfig)>& folderServiceFactory,
    const std::function<IActor*(const NYq::NConfig::TAuditConfig& auditConfig, const NMonitoring::TDynamicCounterPtr& counters)>& auditServiceFactory,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    ui32 icPort,
    const std::vector<NKikimr::NMiniKQL::TComputationNodeFactory>& additionalCompNodeFactories
);

} // NYq
