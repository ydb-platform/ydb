#pragma once

#include "defs.h"
#include "hnsw_index.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <ydb/library/actors/core/actor.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <memory>
#include <utility>
#include <vector>

namespace NKikimr::NDataShard {

// Result of the eager HNSW build, delivered back to the waiting scheme
// transaction via TEvDataShard::TEvAsyncJobComplete (see TRestoreUnit for the
// same pattern). Index is nullptr when the build failed; Error says why.
struct THnswIndexBuildProduct : public IDestructable {
    std::shared_ptr<THnswIndex> Index;
    std::shared_ptr<void> MemoryReservation;
    TString Error;

    THnswIndexBuildProduct(std::shared_ptr<THnswIndex> index,
            std::shared_ptr<void> memoryReservation, TString error)
        : Index(std::move(index))
        , MemoryReservation(std::move(memoryReservation))
        , Error(std::move(error))
    {}
};

// Builds the HNSW graph off the tablet's transaction executor thread and
// reports completion to `replyTo` as TEvAsyncJobComplete with cookie = txId,
// which resumes the suspended scheme transaction.
NActors::IActor* CreateHnswIndexBuildJob(
    const NActors::TActorId& replyTo,
    ui64 txId,
    const Ydb::Table::VectorIndexSettings& settings,
    std::vector<std::pair<TString, TString>> keysAndVectors,
    std::shared_ptr<void> memoryReservation);

} // namespace NKikimr::NDataShard
