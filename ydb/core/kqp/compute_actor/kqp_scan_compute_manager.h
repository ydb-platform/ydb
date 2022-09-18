#pragma once
#include "kqp_compute_state.h"
#include "kqp_scan_compute_stat.h"

namespace NKikimr::NKqp::NComputeActor {

class TInFlightShards: public TScanShardsStatistics {
private:
    using TTabletStates = std::map<ui32, TShardState::TPtr>;
    using TTabletsData = std::map<ui64, TTabletStates>;
    TTabletsData Shards;
    std::map<ui32, ui32> AllocatedGenerations;
    TTabletStates StatesByIndex;
    std::set<ui32> ActualScannerIds;
    ui32 LastGeneration = 0;
    std::map<ui32, TShardState::TPtr> NeedAckStates;

public:
    TString TraceToString() const;

    const std::map<ui32, TShardState::TPtr>& GetNeedAck() const {
        return NeedAckStates;
    }

    void AckSent(TShardState::TPtr state) {
        Y_VERIFY(StatesByIndex.contains(state->ScannerIdx));
        NeedAckStates.erase(state->ScannerIdx);
    }

    void NeedAck(TShardState::TPtr state) {
        Y_VERIFY(StatesByIndex.contains(state->ScannerIdx));
        NeedAckStates.emplace(state->ScannerIdx, state);
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