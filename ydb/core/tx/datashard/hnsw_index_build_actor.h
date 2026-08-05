#pragma once

#include "hnsw_index.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NDataShard {

NActors::IActor* CreateHnswIndexBuildActor(
    const NActors::TActorId& replyTo,
    ui32 localTid,
    ui64 rowCountAtBuild,
    const Ydb::Table::VectorIndexSettings& settings,
    std::vector<std::pair<TString, TString>> keysAndVectors,
    ui64 maxMemoryBytes);

} // namespace NKikimr::NDataShard
