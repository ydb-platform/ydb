#include "columnshard_impl.h"
#include "columnshard_schema.h"
#include "defs.h"

#include "blobs_action/bs/history_cutter.h"
#include "engines/db_wrapper.h"
#include "hooks/abstract/abstract.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

static constexpr ui64 SeedBatchPortionsDefault = 1000;
static constexpr ui64 SeedBatchBytesDefault = 4 * 1024 * 1024;   // 4 MiB
// Upper bound on bytes charged per loader call; flat charger counts bytes only against a nonzero limit.
static constexpr ui64 SeedChargeCeiling = 4 * SeedBatchBytesDefault;

ui64 GetSeedBatchPortions() {
    return NYDBTest::TControllers::GetColumnShardController()->GetSeedBatchPortions(SeedBatchPortionsDefault);
}

ui64 GetSeedBatchBytes() {
    return NYDBTest::TControllers::GetColumnShardController()->GetSeedBatchBytes(SeedBatchBytesDefault);
}

}   // namespace

// Scans the three index tables in bounded batches and feeds each batch to ApplySeedBatch.
class TColumnShard::TTxCutHistorySeed: public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;
    using TCutter = NOlap::NBlobOperations::NBlobStorage::THistoryCutterWrapper;

    const ui64 SeedRun;
    const TMonotonic StartTime;
    // Per-batch: Start and N; N may be adjusted between batches in Complete.
    std::pair<NOlap::TInternalPathId, ui64> StartKey;
    ui64 N;

    // Set by Execute, consumed in Complete.
    bool EndOfRange = false;
    std::pair<NOlap::TInternalPathId, ui64> NextStartKey{ NOlap::TInternalPathId{}, 0 };
    ui64 TotalBytesPrecharged = 0;
    ui64 ExecuteRetries = 0;
    THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> PortionBatch;

    struct TSeedError {
        NOlap::TInternalPathId PathId;
        ui64 PortionId = 0;
        TString Reason;
    };

    std::optional<TSeedError> SeedException;

public:
    TTxCutHistorySeed(TColumnShard* self, ui64 seedRun, std::pair<NOlap::TInternalPathId, ui64> startKey, ui64 n, TMonotonic startTime)
        : TBase(self)
        , SeedRun(seedRun)
        , StartTime(startTime)
        , StartKey(startKey)
        , N(n)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CUT_HISTORY_SEED;
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        // Reset per-attempt output; StartKey and N carry over across page-fault restarts.
        EndOfRange = false;
        TotalBytesPrecharged = 0;
        PortionBatch.clear();
        SeedException.reset();

        NColumnShard::TBlobGroupSelector dsGroupSelector(Self->Info());
        NOlap::TDbWrapper db(txc.DB, &dsGroupSelector);

        // Step 1: load at most N portions from [StartKey, +inf).
        TVector<std::pair<NOlap::TInternalPathId, ui64>> portionIds;
        auto portionsResult = db.LoadPortionsSeeding(StartKey, N, SeedChargeCeiling,
            [&](std::unique_ptr<NOlap::TPortionInfoConstructor>&& p, const NKikimrTxColumnShard::TIndexPortionMeta&) -> bool {
                portionIds.emplace_back(p->GetPathId(), p->GetPortionIdVerified());
                return true;
            });
        if (!portionsResult.Ready) {
            ++ExecuteRetries;
            return false;
        }
        TotalBytesPrecharged += portionsResult.BytesPrecharged;

        if (!portionsResult.LastKey) {
            // Empty table (or start is past the end): seeding completes with an empty batch.
            EndOfRange = true;
            return true;
        }

        const auto to = *portionsResult.LastKey;
        EndOfRange = portionsResult.EndOfRange;

        // Step 2: load IndexColumnsV2 rows over [StartKey, to] to get the full blob vector per portion.
        THashMap<ui64, std::vector<NOlap::TUnifiedBlobId>> colBlobIds;
        auto colResult = db.LoadColumnsSeeding(StartKey, to, SeedChargeCeiling, [&](NOlap::TColumnChunkLoadContextV2&& ctx) {
            auto& vec = colBlobIds[ctx.GetPortionId()];
            for (const auto& blobId : ctx.GetBlobIds()) {
                bool found = false;
                for (const auto& existing : vec) {
                    if (existing == blobId) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    vec.push_back(blobId);
                }
            }
        });
        if (!colResult.Ready) {
            ++ExecuteRetries;
            return false;
        }
        if (!colResult.Error.IsSuccess()) {
            const auto& errKey = colResult.LastKey;
            SeedException = TSeedError{ errKey ? errKey->first : NOlap::TInternalPathId{}, errKey ? errKey->second : 0,
                TString("LoadColumnsSeeding: ") + colResult.Error.GetErrorMessage() };
            return true;
        }
        TotalBytesPrecharged += colResult.BytesPrecharged;

        // Step 3: load IndexIndexes rows over [StartKey, to] for direct-address (legacy) index blobs.
        auto idxResult = db.LoadIndexesSeeding(StartKey, to, SeedChargeCeiling,
            [&](const NOlap::TInternalPathId /*pathId*/, const ui64 portionId, NOlap::TIndexChunkLoadContext&& ctx) {
                if (const auto& bRange = ctx.GetBlobRangeAddress()) {
                    // Direct-address blob: may be absent from the V2 BlobIds list; de-dup and add.
                    auto& vec = colBlobIds[portionId];
                    bool found = false;
                    for (const auto& existing : vec) {
                        if (existing == bRange->BlobId) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        vec.push_back(bRange->BlobId);
                    }
                }
            });
        if (!idxResult.Ready) {
            ++ExecuteRetries;
            return false;
        }
        if (!idxResult.Error.IsSuccess()) {
            const auto& errKey = idxResult.LastKey;
            SeedException = TSeedError{ errKey ? errKey->first : NOlap::TInternalPathId{}, errKey ? errKey->second : 0,
                TString("LoadIndexesSeeding: ") + idxResult.Error.GetErrorMessage() };
            return true;
        }
        TotalBytesPrecharged += idxResult.BytesPrecharged;

        // Check that every portion from step 1 has an IndexColumnsV2 row.
        for (const auto& [pathId, portionId] : portionIds) {
            if (!colBlobIds.contains(portionId)) {
                SeedException = TSeedError{ pathId, portionId, "no IndexColumnsV2 row" };
                return true;
            }
        }

        // Build the batch (portions without any blob IDs are safely absent from the map).
        for (auto& [portionId, blobIds] : colBlobIds) {
            if (!blobIds.empty()) {
                PortionBatch.emplace(portionId, std::move(blobIds));
            }
        }

        // Advance past `to` for the next batch.
        NextStartKey = { to.first, to.second + 1 };
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        TCutter* cutter = Self->CutHistoryCutter;
        if (!cutter || cutter->GetSeedRun() != SeedRun) {
            return;
        }

        const NColumnShard::THistoryCutterCounters& signals = cutter->GetSignals();

        if (ExecuteRetries > 0) {
            for (ui64 i = 0; i < ExecuteRetries; ++i) {
                signals.OnSeedingExecuteRetry();
            }
        }

        if (SeedException) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "cut_history_seed_error")("path_id", SeedException->PathId)(
                "portion_id", SeedException->PortionId)("reason", SeedException->Reason);
            cutter->FailSeeding(SeedException->PathId, SeedException->PortionId, SeedException->Reason);
            return;
        }

        // Apply the batch (may be empty on an empty tablet or when all portions are tombstoned).
        signals.OnSeedingBatch(PortionBatch.size(), TotalBytesPrecharged);
        cutter->ApplySeedBatch(PortionBatch);

        if (EndOfRange) {
            cutter->FinishSeeding();
            const ui64 elapsedMs = (TMonotonic::Now() - StartTime).MilliSeconds();
            signals.OnSeedingDurationMs(elapsedMs);
            return;
        }

        // Batch-size policy (design v12): adjust N from measured cost, single-portion lower bound.
        const ui64 cost = TotalBytesPrecharged;
        const ui64 target = GetSeedBatchBytes();
        const ui64 maxN = GetSeedBatchPortions();
        if (cost > target) {
            N = Max<ui64>(1, N * target / cost);
        } else if (cost > 0 && cost < target / 2) {
            N = Min(maxN, 2 * N);
        }
        // cost == 0 means all rows came from memtables; keep N.

        Self->Execute(new TTxCutHistorySeed(Self, SeedRun, NextStartKey, N, StartTime), ctx);
    }
};

void TColumnShard::BeginCutHistorySeeding(const NActors::TActorContext& ctx) {
    AFL_VERIFY(CutHistoryCutter);
    const ui64 n = GetSeedBatchPortions();
    Execute(new TTxCutHistorySeed(this, CutHistoryCutter->GetSeedRun(), { NOlap::TInternalPathId{}, 0 }, n, TMonotonic::Now()), ctx);
}

}   // namespace NKikimr::NColumnShard
