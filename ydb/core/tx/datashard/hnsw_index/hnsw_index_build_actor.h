#pragma once

#include "hnsw_index.h"
#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/library/actors/core/actor.h>

#include <functional>

namespace NKikimr::NDataShard {

    struct THnswSnapshotScanResult {
        std::vector<std::pair<TString, TString>> Rows;
        std::shared_ptr<void> MemoryReservation;
        ui64 ReservedBytes = 0;
        std::function<void(ui64)> ReduceReservation;
        bool Success = false;
        bool BelowMinRows = false;
        bool AllowEmpty = false;
    };

    using THnswSnapshotScanCallback = std::function<void(THnswSnapshotScanResult&&)>;

    struct THnswIndexBuildResult {
        std::shared_ptr<void> MemoryReservation;
        std::shared_ptr<THnswIndex> Index;
        TString Error;
    };

    using THnswIndexBuildCallback = std::function<void(
        THnswIndexBuildResult&&, const NActors::TActorContext&)>;

    // Common worker used by both eager and lazy builds. The callback only adapts
    // the result to the completion event expected by the caller.
    NActors::IActor* CreateHnswIndexBuildWorker(
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        std::shared_ptr<void> memoryReservation,
        ui64 maxMemoryBytes,
        THnswIndexBuildCallback callback, bool allowEmpty = false,
        std::shared_ptr<std::atomic<bool>> cancelled = {},
        std::function<void(ui64)> reduceReservation = {});

    NActors::IActor* CreateHnswIndexBuildActor(
        const NActors::TActorId& replyTo,
        ui32 localTid,
        ui32 vectorColumnTag,
        ui64 rowCountAtBuild,
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        std::shared_ptr<void> memoryReservation,
        ui64 maxMemoryBytes,
        TRowVersion baseVersion, ui64 buildToken, bool allowEmpty = false, TPathId indexId = {}, ui64 memoryOwner = 0,
        std::shared_ptr<std::atomic<bool>> cancelled = {},
        std::function<void(ui64)> reduceReservation = {});

} // namespace NKikimr::NDataShard
