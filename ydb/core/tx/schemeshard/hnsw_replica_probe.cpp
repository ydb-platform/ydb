#include "hnsw_replica_probe.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/follower_group.pb.h>
#include "schemeshard_private.h"
#include <ydb/core/tx/datashard/hnsw/cache.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NSchemeShard {
namespace {

using namespace NActors;
using NDataShard::TEvHnswCache;
using TCheckpoint = NKikimrHnsw::TReplicaProbeResult::TCheckpoint;

class THnswReplicaProbe : public TActorBootstrapped<THnswReplicaProbe> {
    struct TShard {
        ui64 Hive = 0;
        ui32 Generation = 0;
        ui32 LeaderNode = 0;
        TActorId Leader;
        THashMap<ui32, ui32> ExpectedNodes;
        THashMap<ui32, TActorId> Followers;
        THashSet<TActorId> Pending;
        bool HiveSeen = false;
        bool LookupSeen = false;
        bool TopologyValid = true;
        bool Started = false;
        bool Done = false;
        bool Ready = true;
    };

public:
    THnswReplicaProbe(TActorId replyTo, TOperationId operationId, ui64 round, TPathId pathId,
            ui64 schemaVersion, NKikimrSchemeOp::TPartitionConfig config,
            TVector<std::pair<ui64, ui64>> tabletsAndHives, TVector<TCheckpoint> checkpoints, bool indexBuild)
        : ReplyTo(replyTo), OperationId(operationId), Round(round), PathId(pathId)
        , SchemaVersion(schemaVersion), Config(std::move(config)), IndexBuild(indexBuild)
    {
        // Match Hive's conversion of the legacy replica-count fields.
        if (!Config.FollowerGroupsSize()
                && (Config.HasFollowerCount() || Config.HasCrossDataCenterFollowerCount()
                    || Config.HasAllowFollowerPromotion())) {
            auto* group = Config.AddFollowerGroups();
            group->SetAllowLeaderPromotion(Config.GetAllowFollowerPromotion());
            group->SetAllowClientRead(true);
            if (Config.HasCrossDataCenterFollowerCount()) {
                group->SetFollowerCount(Config.GetCrossDataCenterFollowerCount());
                group->SetFollowerCountPerDataCenter(true);
                group->SetRequireAllDataCenters(true);
            } else {
                group->SetFollowerCount(Config.GetFollowerCount());
            }
        }
        for (const auto& [tablet, hive] : tabletsAndHives)
            Shards[tablet].Hive = hive;
        for (auto& checkpoint : checkpoints)
            Checkpoints[checkpoint.GetTabletId()] = std::move(checkpoint);
    }

    void Bootstrap() {
        if (!AppData()->FeatureFlags.GetEnableHnswIndex()) {
            Reply(false);
            return;
        }
        Become(&TThis::StateWork);
        for (const auto& [tablet, shard] : Shards) {
            RequestHive(tablet, shard.Hive);
            Send(MakeStateStorageProxyID(), new TEvStateStorage::TEvLookup(tablet, tablet));
        }
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
    }

private:
    void RequestHive(ui64 tablet, ui64 hive) {
        auto& pipe = Pipes[hive];
        if (!pipe) {
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            pipe = Register(NTabletPipe::CreateClient(SelfId(), hive, config));
        }
        auto request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
        request->Record.SetTabletID(tablet);
        request->Record.SetReturnFollowers(true);
        NTabletPipe::SendData(SelfId(), pipe, request.Release(), tablet);
    }

    void Handle(TEvHive::TEvResponseHiveInfo::TPtr& ev) {
        auto it = Shards.find(ev->Cookie);
        if (it == Shards.end() || it->second.HiveSeen)
            return;
        const auto& response = ev->Get()->Record;
        if (response.HasForwardRequest()) {
            RequestHive(it->first, response.GetForwardRequest().GetHiveTabletId());
            return;
        }
        auto& shard = it->second;
        shard.HiveSeen = true;
        bool leaderSeen = false;
        for (const auto& tablet : response.GetTablets()) {
            if (tablet.GetTabletID() != it->first)
                continue;
            if (tablet.GetFollowerID()) {
                shard.TopologyValid &= shard.ExpectedNodes.emplace(tablet.GetFollowerID(), tablet.GetNodeID()).second;
                shard.TopologyValid &= tablet.GetNodeID() != 0;
            } else {
                shard.TopologyValid &= !leaderSeen;
                leaderSeen = true;
                shard.LeaderNode = tablet.GetNodeID();
                shard.TopologyValid &= tablet.GetNodeID() != 0 && tablet.HasDesiredFollowers()
                    && tablet.FollowerGroupsSize() == Config.FollowerGroupsSize();
                for (size_t i = 0; i < Min(tablet.FollowerGroupsSize(), Config.FollowerGroupsSize()); ++i) {
                    const auto& actual = tablet.GetFollowerGroups(i);
                    const auto& desired = Config.GetFollowerGroups(i);
                    shard.TopologyValid &= actual.GetAllowLeaderPromotion() == desired.GetAllowLeaderPromotion()
                        && actual.GetAllowClientRead() == desired.GetAllowClientRead()
                        && actual.GetFollowerCount() == desired.GetFollowerCount()
                        && actual.GetFollowerCountPerDataCenter() == desired.GetFollowerCountPerDataCenter()
                        && actual.GetRequireAllDataCenters() == desired.GetRequireAllDataCenters()
                        && actual.GetLocalNodeOnly() == desired.GetLocalNodeOnly()
                        && actual.GetRequireDifferentNodes() == desired.GetRequireDifferentNodes();
                }
                DesiredCounts[it->first] = tablet.GetDesiredFollowers();
            }
        }
        shard.TopologyValid &= leaderSeen
            && shard.ExpectedNodes.size() == DesiredCounts[it->first];
        TryStart(it->first, shard);
    }

    void Handle(TEvStateStorage::TEvInfo::TPtr& ev) {
        const auto& info = *ev->Get();
        auto it = Shards.find(info.TabletID);
        if (it == Shards.end() || it->second.LookupSeen)
            return;
        auto& shard = it->second;
        shard.LookupSeen = true;
        shard.TopologyValid &= info.Status == NKikimrProto::OK && bool(info.CurrentLeaderTablet);
        shard.Leader = info.CurrentLeaderTablet;
        shard.Generation = info.CurrentGeneration;
        for (const auto& follower : info.Followers) {
            if (!follower.FollowerId.Defined() || !follower.FollowerTablet) {
                shard.TopologyValid = false;
                continue;
            }
            shard.TopologyValid &= shard.Followers.emplace(follower.FollowerId.Get(), follower.FollowerTablet).second;
        }
        TryStart(it->first, shard);
    }

    void SendStatus(ui64 tablet, TShard& shard, const TActorId& actor, TRowVersion target) {
        auto request = MakeHolder<TEvHnswCache::TEvStatusRequest>();
        request->Record.SetTableOwnerId(ui64(PathId.OwnerId));
        request->Record.SetTablePathId(ui64(PathId.LocalPathId));
        request->Record.SetSchemaVersion(SchemaVersion);
        request->Record.SetTargetStep(target.Step);
        request->Record.SetTargetTxId(target.TxId);
        shard.Pending.insert(actor);
        Send(actor, request.Release(), IEventHandle::FlagTrackDelivery, tablet);
    }

    void TryStart(ui64 tablet, TShard& shard) {
        if (!shard.HiveSeen || !shard.LookupSeen || shard.Started)
            return;
        shard.Started = true;
        shard.TopologyValid &= shard.Followers.size() == shard.ExpectedNodes.size()
            && shard.Leader.NodeId() == shard.LeaderNode;
        for (const auto& [id, node] : shard.ExpectedNodes) {
            const auto follower = shard.Followers.find(id);
            shard.TopologyValid &= follower != shard.Followers.end()
                && follower->second.NodeId() == node;
        }
        if (!shard.TopologyValid) {
            shard.Done = true;
            shard.Ready = false;
            TryFinish();
            return;
        }
        SendStatus(tablet, shard, shard.Leader, TRowVersion::Min());
    }

    void Handle(TEvHnswCache::TEvStatusResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record;
        auto it = Shards.find(ev->Cookie);
        if (it == Shards.end() || it->second.Done || response.GetTabletId() != it->first)
            return;
        auto& shard = it->second;
        if (!shard.Pending.erase(ev->Sender))
            return;
        const bool valid = AppData()->FeatureFlags.GetEnableHnswIndex()
            && response.HasReadStep() && response.HasReadTxId()
            && (response.GetNotRequired() || !response.GetGraphKey().empty())
            && response.GetLeaderGeneration() == shard.Generation
            && response.GetSchemaVersion() == SchemaVersion && response.GetRepeatable();
        shard.Ready &= valid && response.GetReady();
        if (ev->Sender == shard.Leader) {
            shard.Ready &= response.GetFollowerId() == 0;
            auto checkpoint = Checkpoints.find(it->first);
            if (valid && (checkpoint == Checkpoints.end()
                    || checkpoint->second.GetLeaderGeneration() != shard.Generation)) {
                auto& value = Checkpoints[it->first];
                value.SetTabletId(it->first);
                value.SetLeaderGeneration(shard.Generation);
                value.SetStep(response.GetReadStep());
                value.SetTxId(response.GetReadTxId());
                checkpoint = Checkpoints.find(it->first);
            }
            if (valid && checkpoint != Checkpoints.end()) {
                shard.Ready &= TRowVersion(response.GetReadStep(), response.GetReadTxId())
                    >= TRowVersion(checkpoint->second.GetStep(), checkpoint->second.GetTxId());
            }
            if (shard.Ready) {
                const TRowVersion target(checkpoint->second.GetStep(), checkpoint->second.GetTxId());
                for (const auto& [_, follower] : shard.Followers)
                    SendStatus(it->first, shard, follower, target);
            }
        } else {
            const auto follower = shard.Followers.find(response.GetFollowerId());
            shard.Ready &= follower != shard.Followers.end() && follower->second == ev->Sender;
            const auto checkpoint = Checkpoints.find(it->first);
            shard.Ready &= checkpoint != Checkpoints.end()
                && TRowVersion(response.GetReadStep(), response.GetReadTxId())
                    >= TRowVersion(checkpoint->second.GetStep(), checkpoint->second.GetTxId());
        }
        if (shard.Pending.empty())
            shard.Done = true;
        TryFinish();
    }

    void TryFinish() {
        bool ready = !Shards.empty();
        for (const auto& [_, shard] : Shards) {
            if (!shard.Done)
                return;
            ready &= shard.Ready;
        }
        if (ready) {
            Reply(true);
        } else if (!FinishScheduled) {
            FinishScheduled = true;
            Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
        }
    }

    void HandleWakeup() { Reply(false); }

    void Reply(bool ready) {
        auto response = MakeHolder<TEvPrivate::TEvHnswReplicaStatus>();
        response->IndexBuild = IndexBuild;
        auto& result = response->Record;
        result.SetOperationTxId(ui64(OperationId.GetTxId()));
        result.SetOperationPartId(ui32(OperationId.GetSubTxId()));
        result.SetRound(Round);
        result.SetReady(ready);
        for (const auto& [tablet, _] : Shards) {
            result.AddTablets(tablet);
            if (auto it = Checkpoints.find(tablet); it != Checkpoints.end())
                *result.AddCheckpoints() = it->second;
        }
        Send(ReplyTo, response.Release());
        PassAway();
    }

    void PassAway() override {
        for (const auto& [_, pipe] : Pipes)
            NTabletPipe::CloseClient(SelfId(), pipe);
        TActorBootstrapped::PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHive::TEvResponseHiveInfo, Handle);
            hFunc(TEvStateStorage::TEvInfo, Handle);
            hFunc(TEvHnswCache::TEvStatusResponse, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
            // Missing pipes, old nodes, and undelivered requests remain pending
            // until this round expires; they never count as readiness.
        }
    }

    TActorId ReplyTo;
    TOperationId OperationId;
    ui64 Round;
    TPathId PathId;
    ui64 SchemaVersion;
    NKikimrSchemeOp::TPartitionConfig Config;
    THashMap<ui64, TShard> Shards;
    THashMap<ui64, ui32> DesiredCounts;
    THashMap<ui64, TActorId> Pipes;
    THashMap<ui64, TCheckpoint> Checkpoints;
    bool IndexBuild = false;
    bool FinishScheduled = false;
};

} // namespace

NActors::IActor* CreateHnswReplicaProbe(NActors::TActorId replyTo, TOperationId operationId,
        ui64 round, TPathId pathId, ui64 schemaVersion, NKikimrSchemeOp::TPartitionConfig config,
        TVector<std::pair<ui64, ui64>> tabletsAndHives, TVector<TCheckpoint> checkpoints, bool indexBuild) {
    return new THnswReplicaProbe(replyTo, operationId, round, pathId, schemaVersion,
        std::move(config), std::move(tabletsAndHives), std::move(checkpoints), indexBuild);
}

} // namespace NKikimr::NSchemeShard
