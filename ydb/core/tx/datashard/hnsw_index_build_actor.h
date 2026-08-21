#pragma once

#include "hnsw_index.h"

#include <ydb/library/actors/core/actor.h>

#include <functional>

namespace NKikimr::NDataShard {

struct THnswIndexBuildResult {
    std::shared_ptr<THnswIndex> Index;
    std::shared_ptr<void> MemoryReservation;
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
    THnswIndexBuildCallback callback);

NActors::IActor* CreateHnswIndexBuildActor(
    const NActors::TActorId& replyTo,
    ui32 localTid,
    ui32 vectorColumnTag,
    ui64 rowCountAtBuild,
    const Ydb::Table::VectorIndexSettings& settings,
    std::vector<std::pair<TString, TString>> keysAndVectors,
    std::shared_ptr<void> memoryReservation,
    ui64 maxMemoryBytes);

} // namespace NKikimr::NDataShard
