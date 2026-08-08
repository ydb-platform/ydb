#pragma once

#include "hnsw_index.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>

#include <utility>
#include <vector>

namespace NKikimr::NDataShard {

// Runs THnswIndex::Build off the tablet's transaction executor thread (it is
// CPU-only once the (key, vector) pairs have been collected, so it needs no
// further NTable::TDatabase/page-cache access). Reports the result back to
// `replyTo` as TDataShard::TEvPrivate::TEvHnswIndexBuildResult and dies.
// Register on a non-tablet pool, e.g.:
//   ctx.Register(CreateHnswIndexBuildActor(...), TMailboxType::HTSwap, AppData(ctx)->BatchPoolId)
NActors::IActor* CreateHnswIndexBuildActor(
    const NActors::TActorId& replyTo,
    ui32 localTid,
    ui64 rowCountAtBuild,
    const Ydb::Table::VectorIndexSettings& settings,
    std::vector<std::pair<TString, TString>> keysAndVectors,
    ui64 maxMemoryBytes);

} // namespace NKikimr::NDataShard
