#include "cdc_aggregator_heartbeat_broadcaster.h"
#include "cdc_aggregator_heartbeat_partition_channel.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/utility.h>

#include <utility>
#include <vector>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

namespace {

std::optional<TPathId> FindTopicPathId(const NSchemeCache::TSchemeCacheNavigate::TEntry& stream) {
    if (stream.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok || !stream.ListNodeEntry) {
        return std::nullopt;
    }
    for (const auto& child : stream.ListNodeEntry->Children) {
        if (child.Kind == NSchemeCache::TSchemeCacheNavigate::KindTopic) {
            return child.PathId;
        }
    }
    return std::nullopt;
}

THashMap<ui32, ui64> CollectActivePartitionIdToPersQueueTabletId(const NKikimrSchemeOp::TPersQueueGroupDescription& topic) {
    THashMap<ui32, ui64> activePartitionIdToPersQueueTabletId;
    for (const auto& partition : topic.GetPartitions()) {
        if (partition.GetStatus() == NKikimrPQ::ETopicPartitionStatus::Active) {
            activePartitionIdToPersQueueTabletId.emplace(partition.GetPartitionId(), partition.GetTabletId());
        }
    }
    return activePartitionIdToPersQueueTabletId;
}

} // namespace

class TCdcAggregatorHeartbeatBroadcaster : public TActorBootstrapped<TCdcAggregatorHeartbeatBroadcaster> {
    using EStatus = TCdcAggregatorHeartbeatPartitionChannel::EStatus;

    static constexpr TDuration MinRetryPeriod = TDuration::Seconds(1);
    static constexpr TDuration MaxRetryPeriod = TDuration::Seconds(30);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_TABLET;
    }

    explicit TCdcAggregatorHeartbeatBroadcaster(const TCdcAggregatorStreamTopic& topic)
        : StreamPathId(topic.StreamPathId)
        , StreamPath(topic.StreamPath)
    {
        for (const auto& partition : topic.Partitions) {
            AddChannel(partition.PartitionId, partition.TabletId);
        }
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        RequestTopicPathIdFromSchemeCache();
        Schedule(RetryPeriod, new TEvents::TEvWakeup());
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCdcAggregatorHeartbeatBroadcaster::TEvBroadcastHeartbeat, Handle);
            hFunc(TEvPersQueue::TEvResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated, Handle);
            hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
            IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable);
            hFunc(TEvents::TEvWakeup, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        default:
            YDB_LOG_WARN("Unexpected event",
                {"streamPath", StreamPath},
                {"eventType", ev->GetTypeRewrite()});
        }
    }

    void Handle(TEvCdcAggregatorHeartbeatBroadcaster::TEvBroadcastHeartbeat::TPtr& ev) {
        const auto& heartbeat = ev->Get()->Heartbeat;
        if (Heartbeat && heartbeat.Version <= Heartbeat->Version) {
            return;
        }
        Heartbeat = heartbeat;

        for (const auto& [_, channel] : PartitionIdToChannel) {
            channel->SendHeartbeat(*Heartbeat);
        }
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        const ui32 partitionId = response.GetPartitionResponse().GetCookie();
        const auto channel = FindChannelByPartitionId(partitionId);
        if (!channel || !Heartbeat) {
            return;
        }

        if (channel->HandleResponse(response, *Heartbeat) == EStatus::Fail) {
            YDB_LOG_DEBUG("Heartbeat send failed",
                {"streamPath", StreamPath},
                {"partitionId", partitionId},
                {"response", response.ShortDebugString()});
            FailedPartitionIds.insert(partitionId);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            HandlePipeLost(ev->Get()->TabletId, ev->Get()->ClientId);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        HandlePipeLost(ev->Get()->TabletId, ev->Get()->ClientId);
    }

    void HandlePipeLost(ui64 persQueueTabletId, const TActorId& lostPipe) {
        const auto pipe = PersQueueTabletIdToPipe.find(persQueueTabletId);
        if (pipe == PersQueueTabletIdToPipe.end() || !pipe->second->Reset(lostPipe)) {
            return;
        }

        YDB_LOG_DEBUG("Heartbeat pipe lost",
            {"streamPath", StreamPath},
            {"persQueueTabletId", persQueueTabletId});
        for (const auto partitionId : PersQueueTabletIdToPartitionIds.at(persQueueTabletId)) {
            const auto channel = FindChannelByPartitionId(partitionId);
            if (channel && channel->ForgetSentHeartbeat()) {
                FailedPartitionIds.insert(partitionId);
            }
        }
    }

    void RequestTopicPathIdFromSchemeCache() {
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->ResultSet.emplace_back(NSchemeCache::TSchemeCacheHelpers::MakeNavigateEntry(StreamPathId, NSchemeCache::TSchemeCacheNavigate::OpList));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto topicPathId = FindTopicPathId(ev->Get()->Request->ResultSet.at(0));
        if (!topicPathId) {
            YDB_LOG_WARN("Heartbeat topic path id request failed",
                {"streamPath", StreamPath});
            TopicPathIdRequestFailed = true;
            return;
        }
        SubscribeToTopicChangesInSchemeCache(*topicPathId);
    }

    void SubscribeToTopicChangesInSchemeCache(const TPathId& topicPathId) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(topicPathId));
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        if (!result || !result->GetPathDescription().HasPersQueueGroup()) {
            return;
        }

        const auto activePartitionIdToPersQueueTabletId = CollectActivePartitionIdToPersQueueTabletId(result->GetPathDescription().GetPersQueueGroup());
        RemoveChannelsOfInactivePartitions(activePartitionIdToPersQueueTabletId);
        AddChannelsForNewPartitions(activePartitionIdToPersQueueTabletId);
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr&) {
        RemoveChannelsOfInactivePartitions({});
    }

    void RemoveChannelsOfInactivePartitions(const THashMap<ui32, ui64>& activePartitionIdToPersQueueTabletId) {
        TVector<ui32> inactivePartitionIds;
        for (const auto& [partitionId, _] : PartitionIdToChannel) {
            if (!activePartitionIdToPersQueueTabletId.contains(partitionId)) {
                inactivePartitionIds.push_back(partitionId);
            }
        }
        for (const auto partitionId : inactivePartitionIds) {
            RemoveChannel(partitionId);
        }
    }

    void AddChannelsForNewPartitions(const THashMap<ui32, ui64>& activePartitionIdToPersQueueTabletId) {
        for (const auto& [partitionId, persQueueTabletId] : activePartitionIdToPersQueueTabletId) {
            if (PartitionIdToChannel.contains(partitionId)) {
                continue;
            }
            const auto channel = AddChannel(partitionId, persQueueTabletId);
            if (Heartbeat) {
                channel->SendHeartbeat(*Heartbeat);
            }
        }
    }

    TCdcAggregatorHeartbeatPartitionChannel::TPtr AddChannel(ui32 partitionId, ui64 persQueueTabletId) {
        auto& pipe = PersQueueTabletIdToPipe[persQueueTabletId];
        if (!pipe) {
            pipe = MakeIntrusive<TCdcAggregatorHeartbeatTabletPipe>(persQueueTabletId);
        }
        PersQueueTabletIdToPartitionIds[persQueueTabletId].push_back(partitionId);
        const auto channel = MakeIntrusive<TCdcAggregatorHeartbeatPartitionChannel>(partitionId, pipe);
        PartitionIdToChannel.emplace(partitionId, channel);
        return channel;
    }

    void RemoveChannel(ui32 partitionId) {
        const auto persQueueTabletId = PartitionIdToChannel.at(partitionId)->GetPersQueueTabletId();
        PartitionIdToChannel.erase(partitionId);
        FailedPartitionIds.erase(partitionId);

        auto& partitionIds = PersQueueTabletIdToPartitionIds.at(persQueueTabletId);
        std::erase(partitionIds, partitionId);
        if (!partitionIds.empty()) {
            return;
        }
        PersQueueTabletIdToPipe.at(persQueueTabletId)->Close();
        PersQueueTabletIdToPipe.erase(persQueueTabletId);
        PersQueueTabletIdToPartitionIds.erase(persQueueTabletId);
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        const bool hasFailures = !FailedPartitionIds.empty() || TopicPathIdRequestFailed;
        if (std::exchange(TopicPathIdRequestFailed, false)) {
            RequestTopicPathIdFromSchemeCache();
        }
        for (const auto partitionId : std::exchange(FailedPartitionIds, {})) {
            if (const auto channel = FindChannelByPartitionId(partitionId)) {
                channel->SendHeartbeat(*Heartbeat);
            }
        }
        RetryPeriod = hasFailures ? Min(RetryPeriod * 2, MaxRetryPeriod) : MinRetryPeriod;
        Schedule(RetryPeriod, new TEvents::TEvWakeup());
    }

    TCdcAggregatorHeartbeatPartitionChannel::TPtr FindChannelByPartitionId(ui32 partitionId) const {
        const auto it = PartitionIdToChannel.find(partitionId);
        return it != PartitionIdToChannel.end() ? it->second : nullptr;
    }

    void PassAway() override {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove());
        for (const auto& [_, pipe] : PersQueueTabletIdToPipe) {
            pipe->Close();
        }
        TActorBootstrapped::PassAway();
    }

private:
    const TPathId StreamPathId;
    const TString StreamPath;
    THashMap<ui64, TCdcAggregatorHeartbeatTabletPipe::TPtr> PersQueueTabletIdToPipe;
    THashMap<ui64, TVector<ui32>> PersQueueTabletIdToPartitionIds;
    THashMap<ui32, TCdcAggregatorHeartbeatPartitionChannel::TPtr> PartitionIdToChannel;
    THashSet<ui32> FailedPartitionIds;
    TDuration RetryPeriod = MinRetryPeriod;
    bool TopicPathIdRequestFailed = false;
    std::optional<NPQ::THeartbeat> Heartbeat;
};

IActor* CreateCdcAggregatorHeartbeatBroadcaster(const TCdcAggregatorStreamTopic& topic) {
    return new TCdcAggregatorHeartbeatBroadcaster(topic);
}

} // namespace NKikimr::NCdcAggregator
