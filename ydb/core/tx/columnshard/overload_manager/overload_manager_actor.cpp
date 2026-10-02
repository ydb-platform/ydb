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

void TOverloadManager::RequestNodesList() {
    Send(NActors::GetNameserviceActorId(), new NActors::TEvInterconnect::TEvListNodes());
}

void TOverloadManager::UpdateCompactionOverloadFlag() {
    const bool overloaded = !CompactionOverloadedTablets.empty();
    TOverloadManagerServiceOperator::SetCompactionOverloaded(overloaded);
    Counters.SetCompactionOverloadedTablets(CompactionOverloadedTablets.size());
}

bool TOverloadManager::IsNodeOverloaded() const {
    return !CompactionOverloadedTablets.empty() || TOverloadManagerServiceOperator::IsWriteSideOverloaded();
}

bool TOverloadManager::PublishToFlowControlManagers(NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status) {
    if (!NFlowControl::TFlowControlManagerServiceOperator::IsEnabled()) {
        return false;
    }

    if (CachedNodeIds.empty()) {
        NeedPublicationFlush = true;
        RequestNodesList();
        return false;
    }

    ++OverloadStatusGeneration;
    // Generation is monotonic within a process and starts at Now().GetValue(), so a restarted OM
    // supersedes any LastGeneration a remote FCM retained from the previous incarnation.
    // The publishing node is this actor's Sender (SelfId), not a payload field.
    for (const ui32 nodeId : CachedNodeIds) {
        Send(NFlowControl::TFlowControlManagerServiceOperator::MakeServiceId(nodeId),
            new NFlowControl::TEvNodeOverloadStatus(status, OverloadStatusGeneration));
    }
    LastSentStatus = status;
    NeedPublicationFlush = false;
    return true;
}

void TOverloadManager::SyncPublication(bool force) {
    if (!NFlowControl::TFlowControlManagerServiceOperator::IsEnabled()) {
        return;
    }

    const auto want = IsNodeOverloaded() ? NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED
                                         : NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY;
    // A forced refresh re-publishes the *current* status, including READY. Interconnect drops can
    // lose the one-shot cool edge: an FCM that saw OVERLOADED and missed READY would otherwise keep
    // that node hot forever if we only re-asserted OVERLOADED. Dedup still suppresses unchanged
    // non-forced publishes on the healthy path between refreshes.
    if (!force && !NeedPublicationFlush && LastSentStatus == want) {
        return;
    }
    PublishToFlowControlManagers(want);
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

    const ui64 tabletId = record->GetColumnShardInfo().TabletId;
    if (CompactionOverloadedTablets.erase(tabletId)) {
        UpdateCompactionOverloadFlag();
        Counters.OnCompactionReady();
    }
    SyncPublication(false);
}

void TOverloadManager::Handle(const NOverload::TEvPublishNodeOverloadStatus::TPtr& ev) {
    // Explicit publish (tests / legacy callers). Still records LastSentStatus on success.
    PublishToFlowControlManagers(ev->Get()->GetStatus());
}

void TOverloadManager::Handle(const NOverload::TEvCompactionOverloadState::TPtr& ev) {
    const ui64 tabletId = ev->Get()->GetTabletId();
    const bool overloaded = ev->Get()->GetOverloaded();
    const bool wasEmpty = CompactionOverloadedTablets.empty();

    if (overloaded) {
        CompactionOverloadedTablets.insert(tabletId);
    } else {
        CompactionOverloadedTablets.erase(tabletId);
    }

    const bool nowEmpty = CompactionOverloadedTablets.empty();
    UpdateCompactionOverloadFlag();

    if (wasEmpty && !nowEmpty) {
        Counters.OnCompactionOverload();
    } else if (!wasEmpty && nowEmpty) {
        Counters.OnCompactionReady();
    }
    SyncPublication(false);
}

void TOverloadManager::Handle(const NOverload::TEvSyncNodeOverloadPublication::TPtr&) {
    SyncPublication(false);
}

void TOverloadManager::Handle(const NActors::TEvInterconnect::TEvNodesInfo::TPtr& ev) {
    CachedNodeIds.clear();
    for (const auto& node : ev->Get()->Nodes) {
        CachedNodeIds.insert(node.NodeId);
    }
    // Refresh pushes *current* write+compaction truth, never a stale OVERLOADED snapshot — including
    // READY, so an FCM that missed the cool edge can heal within one NodesListRefreshPeriod.
    SyncPublication(true);
}

void TOverloadManager::Handle(const NActors::TEvents::TEvWakeup::TPtr&) {
    RequestNodesList();
    Schedule(NodesListRefreshPeriod, new NActors::TEvents::TEvWakeup());
}

}   // namespace NKikimr::NColumnShard::NOverload
