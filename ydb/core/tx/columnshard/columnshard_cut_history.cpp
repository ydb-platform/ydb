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

// Runs on the conveyor thread after TTxAskPortionChunks delivers accessor objects.
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

void TColumnShard::SetupCutHistory() {
    if (CutHistoryCutter) {
        CutHistoryCutter->TryNominate(NActors::TActivationContext::AsActorContext());
        return;
    }
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
    // Boot feed starts with EMPTY counters: boot-loaded portions are not counted, so the
    // tier-1 counter can only undercount. Undercount causes a spurious nomination that the
    // tier-2 sweep disproves, or poisons the channel — both fail-safe, never an unsafe cut.
    cutter->OnBootComplete({});
}

void TColumnShard::Handle(TEvPrivate::TEvStartCutHistorySweep::TPtr& /*ev*/, const TActorContext& ctx) {
    if (!CutHistoryCutter) {
        return;
    }
    if (!CutHistoryCutter->IsSweepInFlight()) {
        return;
    }

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
        CutHistoryCutter->OnBatchComplete({}, /*exhausted=*/true, ctx);
        return;
    }

    // A portion deleted since the sweep started cannot pin blobs in an old group.
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
        const auto portion = granule->GetPortionOptional(portionId);
        if (!portion || portion->HasRemoveSnapshot()) {
            continue;
        }
        portionsMap[pathId].UpsertConsumer(NOlap::NBlobOperations::EConsumer::SCAN).AddPortion(portionId);
    }
    if (portionsMap.empty()) {
        CutHistoryCutter->OnBatchComplete({}, isLast, ctx);
        return;
    }

    const auto candidates = CutHistoryCutter->GetActiveSweepCandidates();
    THashMap<TEntryKey, ui32> nextGenMap;
    for (const auto& key : *candidates) {
        nextGenMap.emplace(key, CutHistoryCutter->GetNextFromGeneration(key));
    }

    auto callback = std::make_shared<TCutHistorySweepCallback>(SelfId(), TabletID(), candidates, std::move(nextGenMap), isLast);

    ctx.Send(SelfId(), new TEvPrivate::TEvAskTabletDataAccessors(std::move(portionsMap), callback));
}

void TColumnShard::Handle(TEvPrivate::TEvCutHistorySweepBatchDone::TPtr& ev, const TActorContext& ctx) {
    if (!CutHistoryCutter) {
        return;
    }
    const auto* msg = ev->Get();

    THashSet<TEntryKey> disproved;
    disproved.reserve(msg->Disproved.size());
    for (const auto& [ch, fromGen] : msg->Disproved) {
        disproved.insert(TEntryKey{ ch, fromGen });
    }

    CutHistoryCutter->OnBatchComplete(disproved, msg->Exhausted, ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvCutHistoryBarrierDone::TPtr& ev, const TActorContext& ctx) {
    if (!CutHistoryCutter) {
        return;
    }
    const auto* msg = ev->Get();
    TEntryKey key{ msg->Channel, msg->FromGeneration };
    CutHistoryCutter->OnBarrierResult(key, msg->Ok, ctx.Now());
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
