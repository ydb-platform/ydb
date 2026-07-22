#include "overload_manager_actor.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>

namespace NKikimr::NColumnShard::NOverload {

TOverloadManager::TOverloadManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
    : Counters(countersGroup)
    , OverloadSubscribers(Counters)
{
}

void TOverloadManager::Bootstrap() {
    Become(&TThis::StateMain);
    RequestNodesList();
    Schedule(NodesListRefreshPeriod, new NActors::TEvents::TEvWakeup());
}

bool TOverloadManager::IsCsFlowControlEnabled() {
    return HasAppData() && AppData()->FeatureFlags.GetEnableCsFlowControl();
}

void TOverloadManager::RequestNodesList() {
    Send(NActors::GetNameserviceActorId(), new NActors::TEvInterconnect::TEvListNodes());
}

void TOverloadManager::PublishToFlowControlManagers(NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status) {
    if (!IsCsFlowControlEnabled()) {
        return;
    }

    LastPublishedStatus = status;
    if (CachedNodeIds.empty()) {
        RequestNodesList();
        return;
    }

    ++OverloadStatusGeneration;
    ui32 selfNodeId = SelfId().NodeId();
    if (!selfNodeId && CachedNodeIds.size() == 1) {
        // LocalServices may register OM as TActorId(0, "OverloadMng"); SelfId can still be node-scoped,
        // but if not, single-node caches uniquely identify this node for the status payload.
        selfNodeId = *CachedNodeIds.begin();
    }
    if (!selfNodeId) {
        return;
    }
    for (const ui32 nodeId : CachedNodeIds) {
        Send(NFlowControl::TFlowControlManagerServiceOperator::MakeServiceId(nodeId),
            new NFlowControl::TEvNodeOverloadStatus(selfNodeId, status, OverloadStatusGeneration));
    }
}

void TOverloadManager::Handle(const NOverload::TEvOverloadSubscribe::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.AddOverloadSubscriber(record->GetColumnShardInfo(), record->GetPipeServerInfo(), record->GetOverloadSubscriberInfo());
    TOverloadManagerServiceOperator::NotifyIfResourcesAvailable(true);
    Counters.OnOverloadSubscribe();
}

void TOverloadManager::Handle(const NOverload::TEvOverloadUnsubscribe::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.RemoveOverloadSubscriber(record->GetColumnShardInfo(), record->GetOverloadSubscriberInfo());
    Counters.OnOverloadUnsubscribe();
}

void TOverloadManager::Handle(const NOverload::TEvOverloadPipeServerDisconnected::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.RemovePipeServer(record->GetColumnShardInfo(), record->GetPipeServerInfo());
}

void TOverloadManager::Handle(const NOverload::TEvOverloadResourcesReleased::TPtr&) {
    OverloadSubscribers.NotifyAllOverloadSubscribers();
}

void TOverloadManager::Handle(const NOverload::TEvOverloadColumnShardDied::TPtr& ev) {
    auto record = ev->Get();
    OverloadSubscribers.NotifyColumnShardSubscribers(record->GetColumnShardInfo());
}

void TOverloadManager::Handle(const NOverload::TEvPublishNodeOverloadStatus::TPtr& ev) {
    PublishToFlowControlManagers(ev->Get()->GetStatus());
}

void TOverloadManager::Handle(const NActors::TEvInterconnect::TEvNodesInfo::TPtr& ev) {
    CachedNodeIds.clear();
    for (const auto& node : ev->Get()->Nodes) {
        CachedNodeIds.insert(node.NodeId);
    }
    if (LastPublishedStatus) {
        // Nodes list arrived (or refreshed) while a status is pending / known — (re)push.
        const auto status = *LastPublishedStatus;
        LastPublishedStatus.reset();
        PublishToFlowControlManagers(status);
    }
}

void TOverloadManager::Handle(const NActors::TEvents::TEvWakeup::TPtr&) {
    RequestNodesList();
    Schedule(NodesListRefreshPeriod, new NActors::TEvents::TEvWakeup());
}

}   // namespace NKikimr::NColumnShard::NOverload
