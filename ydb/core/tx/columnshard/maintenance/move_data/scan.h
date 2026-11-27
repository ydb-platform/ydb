#pragma once

#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <util/generic/hash_set.h>

#include <vector>

namespace NKikimr::NColumnShard {

struct TMoveDataScanPortion {
    TInternalPathId PathId;
    ui64 PortionId;
    ui64 AccessorMemory;
};

struct TEvMoveData {
    struct TEvBatch: NActors::TEventLocal<TEvBatch, TEvColumnShard::EvMoveDataBatch> {
        std::vector<TMoveDataScanPortion> Matches;
        std::vector<TMoveDataScanPortion> Missing;
        size_t Rejected = 0;
    };

    struct TEvContinue: NActors::TEventLocal<TEvContinue, EventSpaceBegin(TEvents::ES_PRIVATE)> {
        std::vector<TMoveDataScanPortion> Retry;
    };

    struct TEvWakeup: NActors::TEventLocal<TEvWakeup, TEvColumnShard::EvMoveDataWakeup> {};

    struct TEvPoke: NActors::TEventLocal<TEvPoke, TEvColumnShard::EvMoveDataPoke> {};
};

NActors::IActor* CreateMoveDataScan(
    TActorId owner, std::vector<TMoveDataScanPortion>&& portions, const THashSet<ui32>& groups, ui64 memoryLimit);

}   // namespace NKikimr::NColumnShard
