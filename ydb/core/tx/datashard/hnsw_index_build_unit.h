#pragma once

#include "defs.h"
#include "hnsw_index_build_actor.h"

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
    std::shared_ptr<void> MemoryReservation;
    std::shared_ptr<THnswIndex> Index;
    TString Error;
    ui64 RowCount = 0;
    bool BelowMinRows = false;
    bool Retryable = false;

    THnswIndexBuildProduct(std::shared_ptr<THnswIndex> index,
            std::shared_ptr<void> memoryReservation, TString error)
        : MemoryReservation(std::move(memoryReservation))
        , Index(std::move(index))
        , Error(std::move(error))
    {}
};

struct THnswSnapshotScanProduct : public IDestructable {
    THnswSnapshotScanResult Result;
    explicit THnswSnapshotScanProduct(THnswSnapshotScanResult&& result)
        : Result(std::move(result)) {}
};

// Waits for an executor scan, constructs the graph on the batch pool, and
// resumes the scheme transaction with TEvAsyncJobComplete, cookie = txId.
NActors::IActor* CreateHnswIndexBuildJob(
    const NActors::TActorId& replyTo, ui64 txId,
    const Ydb::Table::VectorIndexSettings& settings);

} // namespace NKikimr::NDataShard
