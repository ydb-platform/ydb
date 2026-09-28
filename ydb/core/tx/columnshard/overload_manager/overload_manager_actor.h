#pragma once

#include <ydb/core/tx/columnshard/overload_manager/overload_manager_counters.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_subscribers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>

#include <optional>

namespace NKikimr::NColumnShard::NOverload {

class TOverloadManager: public NActors::TActorBootstrapped<TOverloadManager> {
    static constexpr TDuration NodesListRefreshPeriod = TDuration::Seconds(60);

    TCSOverloadManagerCounters Counters;
    TOverloadSubscribers OverloadSubscribers;
    THashSet<ui32> CachedNodeIds;
    THashSet<ui64> CompactionOverloadedTablets;
    // Seeded from wall clock at construction so a restarted publisher starts above any generation
    // a surviving remote FCM may have stored in LastGeneration (N28). A counter that restarted at
    // zero would be ignored until it caught up — potentially forever for a cool node.
    ui64 OverloadStatusGeneration = TInstant::Now().GetValue();
    // Last status we successfully pushed to FCMs (not set when push was deferred).
    std::optional<NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus> LastSentStatus;
    bool NeedPublicationFlush = false;

    // clang-format off
    STRICT_STFUNC(StateMain,
                  hFunc(NOverload::TEvOverloadSubscribe, Handle)
                  hFunc(NOverload::TEvOverloadUnsubscribe, Handle)
                  hFunc(NOverload::TEvOverloadPipeServerDisconnected, Handle)
                  hFunc(NOverload::TEvOverloadResourcesReleased, Handle)
                  hFunc(NOverload::TEvOverloadColumnShardDied, Handle)
                  hFunc(NOverload::TEvPublishNodeOverloadStatus, Handle)
                  hFunc(NOverload::TEvCompactionOverloadState, Handle)
                  hFunc(NOverload::TEvSyncNodeOverloadPublication, Handle)
                  hFunc(NActors::TEvInterconnect::TEvNodesInfo, Handle)
                  hFunc(NActors::TEvents::TEvWakeup, Handle)
    )
    // clang-format on

    void Handle(const NOverload::TEvOverloadSubscribe::TPtr& ev);
    void Handle(const NOverload::TEvOverloadUnsubscribe::TPtr& ev);
    void Handle(const NOverload::TEvOverloadPipeServerDisconnected::TPtr& ev);
    void Handle(const NOverload::TEvOverloadResourcesReleased::TPtr& ev);
    void Handle(const NOverload::TEvOverloadColumnShardDied::TPtr& ev);
    void Handle(const NOverload::TEvPublishNodeOverloadStatus::TPtr& ev);
    void Handle(const NOverload::TEvCompactionOverloadState::TPtr& ev);
    void Handle(const NOverload::TEvSyncNodeOverloadPublication::TPtr& ev);
    void Handle(const NActors::TEvInterconnect::TEvNodesInfo::TPtr& ev);
    void Handle(const NActors::TEvents::TEvWakeup::TPtr& ev);

    void RequestNodesList();
    void UpdateCompactionOverloadFlag();
    bool IsNodeOverloaded() const;
    void SyncPublication(bool force);
    bool PublishToFlowControlManagers(NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status);

public:
    TOverloadManager(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);
    void Bootstrap();
};

}   // namespace NKikimr::NColumnShard::NOverload
