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
    TCutHistorySweepCallback(const TActorId& tabletActorId, ui64 ourTabletId, const std::shared_ptr<const TVector<TEntryKey>>& candidates,
        THashMap<TEntryKey, ui32>&& nextGenMap, bool exhausted)
        : TabletActorId(tabletActorId)
        , OurTabletId(ourTabletId)
        , Candidates(candidates)
        , NextGenMap(std::move(nextGenMap))
        , Exhausted(exhausted)
    {
    }

    void OnAccessorsFetched(std::vector<std::shared_ptr<NOlap::TPortionDataAccessor>>&& accessors) override {
        THashSet<TEntryKey> disprovedKeys;

        // Group candidates by channel once: blobs then check only their channel's slice.
        THashMap<ui32, TVector<TEntryKey>> candidatesByChannel;
        for (const auto& key : *Candidates) {
            candidatesByChannel[key.Channel].push_back(key);
        }

        for (const auto& accessor : accessors) {
            if (!accessor) {
                continue;
            }
            if (disprovedKeys.size() == Candidates->size()) {
                break;
            }
            for (const auto& blobId : accessor->GetBlobIds()) {
                const TLogoBlobID& logoBlobId = blobId.GetLogoBlobId();
                // Skip blobs from other tablets (shared/borrowed) — they must not disprove our candidates.
                if (logoBlobId.TabletID() != OurTabletId) {
                    continue;
                }
                const auto* channelCandidates = candidatesByChannel.FindPtr(logoBlobId.Channel());
                if (!channelCandidates) {
                    continue;
                }
                for (const auto& key : *channelCandidates) {
                    if (disprovedKeys.contains(key)) {
                        continue;
                    }
                    const ui32 gen = logoBlobId.Generation();
                    if (gen < key.FromGeneration) {
                        continue;
                    }
                    if (const auto* nextGen = NextGenMap.FindPtr(key); nextGen && gen < *nextGen) {
                        disprovedKeys.emplace(key);
                    }
                }
            }
        }

        TVector<std::pair<ui32, ui32>> disproved;
        disproved.reserve(disprovedKeys.size());
        for (const auto& key : disprovedKeys) {
            disproved.emplace_back(key.Channel, key.FromGeneration);
        }

        NActors::TActivationContext::AsActorContext().Send(
            TabletActorId, new TEvPrivate::TEvCutHistorySweepBatchDone(std::move(disproved), Exhausted));
    }

private:
    TActorId TabletActorId;
    ui64 OurTabletId;
    std::shared_ptr<const TVector<TEntryKey>> Candidates;
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
    // Boot feed starts with EMPTY counters, and that is safe by direction of drift:
    //   • OnPortionAdded hooks fire only for portions written after boot (compaction/TTL
    //     output via TChangesWithAppend); boot-loaded portions are NOT counted. Undercount
    //     can cause a spurious nomination, which the tier-2 sweep then disproves — wasted
    //     work (bounded by DisprovedRetryCooldown), never an unsafe cut.
    //   • Removal of an uncounted portion drives the counter to zero/poison — poisoning
    //     excludes the channel (fail-safe liveness loss, not a safety loss).
    //   • The authoritative check before any barrier is the tier-2 sweep + final re-check.
    cutter->OnBootComplete({});
}

void TColumnShard::Handle(TEvPrivate::TEvStartCutHistorySweep::TPtr& /*ev*/, const TActorContext& ctx) {
    if (!CutHistoryCutter) {
        return;
    }
    if (!CutHistoryCutter->IsSweepInFlight()) {
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

    // Build per-path consumer map. The snapshot may reference portions (or whole
    // paths) deleted since the sweep started — filter them out here, and the fetch
    // tx additionally tolerates the erase-committed-but-still-in-memory window.
    // A deleted portion cannot pin blobs in an old group, so skipping is correct.
    if (!HasIndex()) {
        CutHistoryCutter->OnBatchComplete({}, /*exhausted=*/true, ctx);
        return;
    }
    const auto& engine = GetIndexAs<NOlap::TColumnEngineForLogs>();
    THashMap<NOlap::TInternalPathId, NOlap::NDataAccessorControl::TPortionsByConsumer> portionsMap;
    for (const auto& [pathId, portionId] : batch) {
        const auto granule = engine.GetGranuleOptional(pathId);
        if (!granule) {
            continue;
        }
        const auto portion = granule->GetPortionOptional(portionId, false);
        if (!portion || portion->HasRemoveSnapshot()) {
            continue;
        }
        portionsMap[pathId].UpsertConsumer(NOlap::NBlobOperations::EConsumer::SCAN).AddPortion(portionId);
    }
    if (portionsMap.empty()) {
        // Every portion of this batch is gone — report an empty batch instead of
        // sending a vacuous accessor request.
        CutHistoryCutter->OnBatchComplete({}, isLast, ctx);
        return;
    }

    // Build nextGenMap for the callback over the entries still alive in this sweep:
    // earlier batches' disprovals shrink the set, so later batches skip them.
    const auto candidates = CutHistoryCutter->GetActiveSweepCandidates();
    THashMap<TEntryKey, ui32> nextGenMap;
    for (const auto& key : *candidates) {
        nextGenMap.emplace(key, CutHistoryCutter->GetNextFromGenerationForSweep(key));
    }

    auto callback = std::make_shared<TCutHistorySweepCallback>(SelfId(), TabletID(), candidates, std::move(nextGenMap), isLast);

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
