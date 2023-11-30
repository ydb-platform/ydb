#pragma once
#include "kqp_compute_state.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log_iface.h>

namespace NKikimr::NKqp::NScanPrivate {

using TShardState = NComputeActor::TShardState;

bool IsDebugLogEnabled(const NActors::TActorSystem* actorSystem, NActors::NLog::EComponent component);

struct TScannedDataStats {
    std::map<ui64, std::pair<ui64, ui64>> ReadShardInfo;
    ui64 CompletedShards = 0;
    ui64 TotalReadRows = 0;
    ui64 TotalReadBytes = 0;

    TScannedDataStats() = default;

    void AddReadStat(const ui64 tabletId, const ui64 rows, const ui64 bytes) {
        auto [it, success] = ReadShardInfo.emplace(tabletId, std::make_pair(rows, bytes));
        if (!success) {
            auto& [currentRows, currentBytes] = it->second;
            currentRows += rows;
            currentBytes += bytes;
        }
    }

    void CompleteShard(TShardState::TPtr state) {
        auto it = ReadShardInfo.find(state->TabletId);
        AFL_ENSURE(it != ReadShardInfo.end());
        auto& [currentRows, currentBytes] = it->second;
        TotalReadRows += currentRows;
        TotalReadBytes += currentBytes;
        ++CompletedShards;
        ReadShardInfo.erase(it);
    }

    ui64 AverageReadBytes() const {
        return (CompletedShards == 0) ? 0 : TotalReadBytes / CompletedShards;
    }

    ui64 AverageReadRows() const {
        return (CompletedShards == 0) ? 0 : TotalReadRows / CompletedShards;
    }
};

}
