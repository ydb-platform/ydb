#pragma once
#include "kqp_scan_common.h"
#include "kqp_compute_actor.h"
#include "kqp_compute_state.h"
#include "kqp_scan_compute_stat.h"
#include <ydb/core/base/appdata.h>
#include <library/cpp/actors/wilson/wilson_profile_span.h>

namespace NKikimr::NKqp::NScanPrivate {

class TInFlightComputes {
public:
    class TWaitingShard {
    private:
        YDB_READONLY_DEF(TShardState::TPtr, ShardState);
    public:
        explicit TWaitingShard(TShardState::TPtr shardState)
            : ShardState(shardState) {

        }
    };

    class TFreeComputeActor {
    private:
        YDB_READONLY_DEF(NActors::TActorId, ActorId);
        YDB_READONLY(ui64, FreeSpace, Max<ui64>());
    public:
        bool operator<(const TFreeComputeActor& item) const {
            return FreeSpace < item.FreeSpace;
        }

        TFreeComputeActor(const NActors::TActorId& actorId, const ui64 freeSpace)
            : ActorId(actorId)
            , FreeSpace(freeSpace) {

        }
    };

private:
    std::set<NActors::TActorId> ComputeActorsFree;
    std::vector<TFreeComputeActor> ComputeActorsWaitShard;
    std::map<ui64, TFreeComputeActor> ComputeActorsWaitData;

    std::set<ui64> ShardsWaitingIds;
    std::deque<TWaitingShard> ShardsWaiting;

    void DetachComputeActorFromShard(const ui64 tabletId) {
        ShardsWaitingIds.erase(tabletId);

        const auto pred = [tabletId](const TWaitingShard& shard) {
            return shard.GetShardState()->TabletId == tabletId;
        };
        ShardsWaiting.erase(std::remove_if(ShardsWaiting.begin(), ShardsWaiting.end(), pred), ShardsWaiting.end());

        auto it = ComputeActorsWaitData.find(tabletId);
        if (it != ComputeActorsWaitData.end()) {
            ComputeActorsWaitShard.emplace_back(std::move(it->second));
            ComputeActorsWaitData.erase(it);
            std::push_heap(ComputeActorsWaitShard.begin(), ComputeActorsWaitShard.end());
        }
    }

public:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "free_ca=" << ComputeActorsFree.size() << ";" <<
              "ca_wait_shard=" << ComputeActorsWaitShard.size() << ";" <<
              "ca_wait_data=" << ComputeActorsWaitData.size() << ";" <<
              "shards_waiting=" << ShardsWaiting.size() << ";";
        return sb;
    }

    bool OnComputeAck(const TActorId& computeActorId, const ui64 freeSpace) {
        if (!ComputeActorsFree.emplace(computeActorId).second) {
            return false;
        }
        ComputeActorsWaitShard.emplace_back(TFreeComputeActor(computeActorId, freeSpace));
        std::push_heap(ComputeActorsWaitShard.begin(), ComputeActorsWaitShard.end());
        return true;
    }

    bool StopReadShard(const ui64 tabletId) {
        DetachComputeActorFromShard(tabletId);
        const auto pred = [tabletId](const TWaitingShard& w) {
            return w.GetShardState()->TabletId == tabletId;
        };
        ShardsWaiting.erase(std::remove_if(ShardsWaiting.begin(), ShardsWaiting.end(), pred), ShardsWaiting.end());
        return ComputeActorsWaitShard.size();
    }

    bool ExtractWaitingForProvide(std::optional<TWaitingShard>& result) {
        if (ShardsWaiting.empty()) {
            result.reset();
            return true;
        }
        if (ComputeActorsWaitShard.empty()) {
            return false;
        } else {
            Y_VERIFY(ShardsWaitingIds.erase(ShardsWaiting.front().GetShardState()->TabletId));
            result = std::move(ShardsWaiting.front());
            ShardsWaiting.pop_front();
            return true;
        }
    }

    TFreeComputeActor OnDataReceived(const ui64 tabletId, const bool readRequestedSpaceLimit) {
        auto it = ComputeActorsWaitData.find(tabletId);
        Y_VERIFY(it != ComputeActorsWaitData.end());
        if (readRequestedSpaceLimit) {
            TFreeComputeActor computeActorInfo = std::move(it->second);
            ComputeActorsWaitData.erase(it);
            ComputeActorsFree.erase(computeActorInfo.GetActorId());
            return std::move(computeActorInfo);
        } else {
            return it->second;
        }
    }

    void OnScanError(const ui64 tabletId) {
        DetachComputeActorFromShard(tabletId);
    }

    void OnEmptyDataReceived(const ui64 tabletId) {
        DetachComputeActorFromShard(tabletId);
    }

    bool PrepareShardAck(TShardState::TPtr state, ui64& freeSpace) {
        if (ComputeActorsWaitShard.empty()) {
            Y_VERIFY(ShardsWaitingIds.emplace(state->TabletId).second);
            ShardsWaiting.emplace_back(TWaitingShard(state));
            return false;
        }
        std::pop_heap(ComputeActorsWaitShard.begin(), ComputeActorsWaitShard.end());
        freeSpace = ComputeActorsWaitShard.back().GetFreeSpace();
        Y_VERIFY(ComputeActorsWaitData.emplace(state->TabletId, std::move(ComputeActorsWaitShard.back())).second);
        ComputeActorsWaitShard.pop_back();
        return true;
    }
};

class TInFlightShards: public NComputeActor::TScanShardsStatistics {
private:
    using TTabletStates = std::map<ui32, TShardState::TPtr>;
    using TTabletsData = std::map<ui64, TTabletStates>;
    TTabletsData Shards;
    std::map<ui32, ui32> AllocatedGenerations;
    TTabletStates StatesByIndex;
    std::set<ui32> ActualScannerIds;
    ui32 LastGeneration = 0;
    std::map<ui32, TShardState::TPtr> NeedAckStates;
    std::set<ui64> AffectedShards;
    const TShardsScanningPolicy& ScanningPolicy;
    bool IsActiveFlag = true;
    NWilson::TProfileSpan& KqpProfileSpan;

public:
    TInFlightShards(const TShardsScanningPolicy& scanningPolicy, NWilson::TProfileSpan& kqpProfileSpan)
        : ScanningPolicy(scanningPolicy)
        , KqpProfileSpan(kqpProfileSpan)
    {
        Y_UNUSED(ScanningPolicy);
        Y_UNUSED(KqpProfileSpan);
    }
    ui32 GetAvailableTasks() const {
        return GetScansCount();
    }
    bool IsActive() const {
        return IsActiveFlag;
    }
    void Stop() {
        Y_VERIFY(GetAvailableTasks() == 0);
        IsActiveFlag = false;
    }
    void ClearAll();
    const std::set<ui64>& GetAffectedShards() const {
        return AffectedShards;
    }

    TString TraceToString() const;

    const std::map<ui32, TShardState::TPtr>& GetNeedAck() const {
        return NeedAckStates;
    }

    void ClearAckState(TShardState::TPtr state) {
        auto it = NeedAckStates.find(state->ScannerIdx);
        if (it != NeedAckStates.end()) {
            NeedAckStates.erase(it);
        }
    }

    void AckSent(TShardState::TPtr state) {
        Y_VERIFY(StatesByIndex.contains(state->ScannerIdx));
        NeedAckStates.erase(state->ScannerIdx);
    }

    void NeedAck(TShardState::TPtr state) {
        Y_VERIFY(StatesByIndex.contains(state->ScannerIdx));
        NeedAckStates.emplace(state->ScannerIdx, state);
        AffectedShards.emplace(state->TabletId);
    }

    ui32 AllocateGeneration(TShardState::TPtr state);
    ui32 GetScansCount() const;
    ui32 GetShardsCount() const {
        return Shards.size();
    }
    bool empty() const {
        return Shards.empty();
    }
    TTabletsData::const_iterator begin() const {
        return Shards.begin();
    }
    TTabletsData::const_iterator end() const {
        return Shards.end();
    }
    TShardState::TPtr GetStateByIndex(const ui32 index) const {
        auto it = StatesByIndex.find(index);
        if (it == StatesByIndex.end()) {
            return nullptr;
        }
        return it->second;
    }
    ui32 GetIndexByGeneration(const ui32 generation);
    TShardState::TPtr RemoveIfExists(TShardState::TPtr state);

    TShardState::TPtr RemoveIfExists(const ui32 scannerIdx);
    TShardState::TPtr Put(TShardState&& state);
    const TTabletStates& GetByTabletId(const ui64 tabletId) const {
        auto it = Shards.find(tabletId);
        if (it == Shards.end()) {
            return Default<TTabletStates>();
        } else {
            return it->second;
        }
    }
    TTabletStates* MutableByTabletId(const ui64 tabletId) {
        auto it = Shards.find(tabletId);
        if (it == Shards.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }
};

}
