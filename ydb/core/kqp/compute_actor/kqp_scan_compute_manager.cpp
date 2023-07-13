#include "kqp_scan_compute_manager.h"
#include <ydb/library/wilson_ids/wilson.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp::NScanPrivate {

TString TInFlightShards::TraceToString() const {
    TStringBuilder sb;
    for (auto&& i : StatesByIndex) {
        sb << i.first << ":" << i.second->State << ":" << NeedAckStates.contains(i.first) << ";";
    }
    return sb;
}

TShardState::TPtr TInFlightShards::RemoveIfExists(const ui32 scannerIdx) {
    if (!scannerIdx) {
        return nullptr;
    }
    auto itScanner = StatesByIndex.find(scannerIdx);
    if (itScanner == StatesByIndex.end()) {
        return nullptr;
    }
    TShardState::TPtr result = itScanner->second;
    auto itTablet = Shards.find(result->TabletId);
    if (itTablet == Shards.end()) {
        return nullptr;
    }
    auto it = itTablet->second.find(result->ScannerIdx);
    if (it == itTablet->second.end()) {
        return nullptr;
    }
    MutableStatistics(result->TabletId).MutableStatistics(result->ScannerIdx).SetFinishInstant(Now());
    NeedAckStates.erase(result->ScannerIdx);
    TScanShardsStatistics::OnScansDiff(Shards.size(), GetScansCount());

    itTablet->second.erase(it);
    if (itTablet->second.empty()) {
        Shards.erase(itTablet);
    }
    StatesByIndex.erase(itScanner);
    return result;
}

TShardState::TPtr TInFlightShards::RemoveIfExists(TShardState::TPtr state) {
    if (!state) {
        return state;
    }
    return RemoveIfExists(state->ScannerIdx);
}

TShardState::TPtr TInFlightShards::Put(TShardState&& state) {
    TScanShardsStatistics::OnScansDiff(Shards.size(), GetScansCount());
    MutableStatistics(state.TabletId).MutableStatistics(state.ScannerIdx).SetStartInstant(Now());

    TShardState::TPtr result = std::make_shared<TShardState>(std::move(state));
    StatesByIndex.emplace(result->ScannerIdx, result);
    Shards[result->TabletId].emplace(result->ScannerIdx, result);
    return result;
}

ui32 TInFlightShards::GetIndexByGeneration(const ui32 generation) {
    auto it = AllocatedGenerations.find(generation);
    if (it == AllocatedGenerations.end()) {
        return 0;
    }
    return it->second;
}

ui32 TInFlightShards::AllocateGeneration(TShardState::TPtr state) {
    {
        auto itTablet = Shards.find(state->TabletId);
        Y_VERIFY(itTablet != Shards.end());
        auto it = itTablet->second.find(state->ScannerIdx);
        Y_VERIFY(it != itTablet->second.end());
    }

    const ui32 nextGeneration = ++LastGeneration;
    Y_VERIFY(AllocatedGenerations.emplace(nextGeneration, state->ScannerIdx).second);
    return nextGeneration;
}

ui32 TInFlightShards::GetScansCount() const {
    ui32 result = 0;
    for (auto&& i : Shards) {
        result += i.second.size();
    }
    return result;
}

void TInFlightShards::ClearAll() {
    Shards.clear();
    AllocatedGenerations.clear();
    StatesByIndex.clear();
    NeedAckStates.clear();
}
}
