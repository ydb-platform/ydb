#pragma once

#include "hnsw_index.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NDataShard {

NActors::IActor* CreateHnswIndexBuildActor(
    const NActors::TActorId& replyTo,
    ui32 localTid,
    ui32 vectorColumnTag,
    ui64 rowCountAtBuild,
    const Ydb::Table::VectorIndexSettings& settings,
    std::vector<std::pair<TString, TString>> keysAndVectors,
    std::shared_ptr<void> memoryReservation);

} // namespace NKikimr::NDataShard
