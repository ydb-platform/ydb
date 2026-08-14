#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/blobs_action/bs/history_cutter.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/data_accessor/abstract/collector.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

#include <ydb/library/actors/core/actor.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

namespace {

using TEntryKey = NOlap::NBlobOperations::NBlobStorage::TEntryKey;
using THistoryCutterWrapper = NOlap::NBlobOperations::NBlobStorage::THistoryCutterWrapper;

// ---- TCutHistorySweepCallback -----------------------------------------------
//
// Runs on the conveyor thread after TTxAskPortionChunks delivers accessor objects.
// Inspects each portion's blob IDs against the current sweep candidates and
// sends TEvCutHistorySweepBatchDone back to the tablet.
//
class TCutHistorySweepCallback: public NOlap::NDataAccessorControl::IAccessorCallback {
public:
    TCutHistorySweepCallback(
        const TActorId& tabletActorId, ui64 ourTabletId, TVector<TEntryKey>&& candidates, THashMap<TEntryKey, ui32>&& nextGenMap, bool exhausted)
        : TabletActorId(tabletActorId)
        , OurTabletId(ourTabletId)
        , Candidates(std::move(candidates))
        , NextGenMap(std::move(nextGenMap))
        , Exhausted(exhausted)
    {
    }

    void OnAccessorsFetched(std::vector<std::shared_ptr<NOlap::TPortionDataAccessor>>&& accessors) override {
        TVector<std::pair<ui32, ui32>> disproved;

        for (const auto& accessor : accessors) {
            if (!accessor) {
                continue;
            }
            for (const auto& blobId : accessor->GetBlobIds()) {
                const TLogoBlobID& lid = blobId.GetLogoBlobId();
                // Skip blobs from other tablets (shared/borrowed) — they must not disprove our candidates.
                if (lid.TabletID() != OurTabletId) {
                    continue;
                }
                for (const auto& key : Candidates) {
                    if (lid.Channel() != key.Channel) {
                        continue;
                    }
                    const ui32 gen = lid.Generation();
                    if (gen < key.FromGeneration) {
                        continue;
                    }
                    const auto it = NextGenMap.find(key);
                    if (it != NextGenMap.end() && gen < it->second) {
                        disproved.emplace_back(key.Channel, key.FromGeneration);
                    }
                }
            }
        }

        // Deduplicate disproved (multiple blobs from same entry).
        std::sort(disproved.begin(), disproved.end());
        disproved.erase(std::unique(disproved.begin(), disproved.end()), disproved.end());

        NActors::TActivationContext::AsActorContext().Send(
            TabletActorId, new TEvPrivate::TEvCutHistorySweepBatchDone(std::move(disproved), Exhausted));
    }

private:
    TActorId TabletActorId;
    ui64 OurTabletId;
    TVector<TEntryKey> Candidates;
    THashMap<TEntryKey, ui32> NextGenMap;
    bool Exhausted;
};

}   // anonymous namespace

// ---- TColumnShard methods ---------------------------------------------------

void TColumnShard::SetupCutHistory() {
    if (CutHistoryCutter) {
        // Periodic nomination trigger.
        CutHistoryCutter->TryNominate(NActors::TActivationContext::AsActorContext());
        return;
    }
    // One-time initialization on first call (from TrySwitchToWork).
    auto op = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(
        StoragesManager->GetOperatorOptional(NOlap::IStoragesManager::DefaultStorageId));
    if (!op) {
        return;
    }
    op->InitHistoryCutter(SelfId());
    auto* cutter = op->GetHistoryCutter();
    if (!cutter) {
        return;
    }
    cutter->SetLauncherActorId(LauncherID());
    CutHistoryCutter = cutter;
    // Boot feed with empty map is correct and safe:
    //   • Old live portions are already in the engine at boot → tier-2 sweep will see them and
    //     disprove any candidate whose channel/generation range they touch.
    //   • Portions mid-delete have blobs in BlobsToDelete/Delayed → IsDrained returns false,
    //     blocking nomination until regular GC completes.
    //   • Fully-GCed historical ranges have no blobs anywhere → the barrier request is vacuous
    //     and succeeds immediately (or returns ALREADY, treated as success).
    // Counter state is rebuilt on-the-fly by OnPortionAdded hooks as the engine loads.
    cutter->OnBootComplete({});
}

void TColumnShard::Handle(TEvPrivate::TEvStartCutHistorySweep::TPtr& /*ev*/, const TActorContext& ctx) {
    if (!CutHistoryCutter) {
        return;
    }
    if (!CutHistoryCutter->SweepInFlight()) {
        return;
    }

    // Snapshot engine portion list on first batch of this sweep.
    if (!CutHistoryCutter->HasPortionSnapshot()) {
        TVector<std::pair<NOlap::TInternalPathId, ui64>> ids;
        if (HasIndex()) {
            const auto& idx = GetIndexAs<NOlap::TColumnEngineForLogs>();
            for (const auto& [pathId, granule] : idx.GetTables()) {
                for (const auto& [portionId, _] : granule->GetPortions()) {
                    ids.emplace_back(pathId, portionId);
                }
            }
        }
        CutHistoryCutter->SetPortionSnapshot(std::move(ids));
    }

    bool isLast = false;
    auto batch = CutHistoryCutter->GetNextBatch(/*batchSize=*/1000, isLast);

    if (batch.empty()) {
        // No portions — treat as fully exhausted.
        CutHistoryCutter->OnBatchComplete({}, /*exhausted=*/true, ctx);
        return;
    }

    // Build per-path consumer map.
    THashMap<NOlap::TInternalPathId, NOlap::NDataAccessorControl::TPortionsByConsumer> portionsMap;
    for (const auto& [pathId, portionId] : batch) {
        portionsMap[pathId].UpsertConsumer(NOlap::NBlobOperations::EConsumer::SCAN).AddPortion(portionId);
    }

    // Build nextGenMap for the callback.
    const auto& candidates = CutHistoryCutter->GetSweepCandidates();
    THashMap<TEntryKey, ui32> nextGenMap;
    for (const auto& key : candidates) {
        nextGenMap.emplace(key, CutHistoryCutter->GetNextFromGenerationPublic(key));
    }

    auto callback =
        std::make_shared<TCutHistorySweepCallback>(SelfId(), TabletID(), TVector<TEntryKey>(candidates), std::move(nextGenMap), isLast);

    ctx.Send(SelfId(), new TEvPrivate::TEvAskTabletDataAccessors(std::move(portionsMap), callback));
}

void TColumnShard::Handle(TEvPrivate::TEvCutHistorySweepBatchDone::TPtr& ev, const TActorContext& ctx) {
    if (!CutHistoryCutter) {
        return;
    }
    const auto* msg = ev->Get();

    // Convert flat pairs to THashSet<TEntryKey>.
    THashSet<TEntryKey> disproved;
    disproved.reserve(msg->Disproved.size());
    for (const auto& [ch, fromGen] : msg->Disproved) {
        disproved.insert(TEntryKey{ ch, fromGen });
    }

    CutHistoryCutter->OnBatchComplete(disproved, msg->Exhausted, ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvCutHistoryBarrierDone::TPtr& ev, const TActorContext& /*ctx*/) {
    if (!CutHistoryCutter) {
        return;
    }
    const auto* msg = ev->Get();
    TEntryKey key{ msg->Channel, msg->FromGeneration };
    CutHistoryCutter->OnBarrierResult(key, msg->Ok);
}

void TColumnShard::OnPortionAddedToEngine(const NOlap::TPortionDataAccessor& accessor) {
    if (CutHistoryCutter) {
        CutHistoryCutter->OnPortionAdded(accessor);
    }
}

void TColumnShard::OnPortionRemovedFromEngine(const ui64 portionId) {
    if (CutHistoryCutter) {
        CutHistoryCutter->OnPortionRemoved(portionId);
    }
}

}   // namespace NKikimr::NColumnShard
