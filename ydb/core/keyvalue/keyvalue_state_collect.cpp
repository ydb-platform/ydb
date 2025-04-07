#include "keyvalue_state.h"
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

void TKeyValueState::PrepareCollectIfNeeded(const TActorContext &ctx) {
    ALOG_TRACE(NKikimrServices::KEYVALUE, "PrepareCollectIfNeeded KeyValue# " << TabletId << " Marker# KV61");

    CleanupEmptyTrashBins(ctx);
    auto& trashBin = GetCollectingTrashBin();
    if (CmdTrimLeakedBlobsUids || IsCollectEventSent || trashBin.empty()) { // can't start GC right now
        return;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // calculate maximum blob id in trash
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    const TLogoBlobID minTrashId = *trashBin.begin();
    const TLogoBlobID maxTrashId = *--trashBin.end();
    if (THelpers::GenerationStep(minTrashId) == THelpers::TGenerationStep(ExecutorGeneration, NextLogoBlobStep) &&
            InFlightForStep.contains(NextLogoBlobStep)) {
        // do not generate more blobs with this NextLogoBlobStep as they are already fully blocking tablet from GC
        Step();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // derive new collect step for this operation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    THelpers::TGenerationStep inflightGenStep(Max<ui32>(), Max<ui32>());
    if (InFlightForStep) {
        const auto& [step, _] = *InFlightForStep.begin();
        Y_ABORT_UNLESS(step);
        inflightGenStep = THelpers::TGenerationStep(ExecutorGeneration, step - 1);
    }
    const auto storedGenStep = THelpers::TGenerationStep(StoredState.GetCollectGeneration(), StoredState.GetCollectStep());
    const auto requiredGenStep = Max(storedGenStep, THelpers::GenerationStep(maxTrashId));
    const auto collectGenStep = Min(inflightGenStep, requiredGenStep);
    Y_ABORT_UNLESS(THelpers::TGenerationStep(ExecutorGeneration, 0) <= collectGenStep ||
        collectGenStep == THelpers::TGenerationStep(ExecutorGeneration - 1, Max<ui32>()));
    Y_ABORT_UNLESS(storedGenStep <= collectGenStep);

    // check if it is useful to start any collection
    if (collectGenStep < THelpers::GenerationStep(minTrashId)) {
        return; // we do not have the opportunity to collect anything here with this allowed barrier
    }

    // create basic collect operation with zero keep/doNotKeep flag vectors; they will be calculated just before sending
    CollectOperation.Reset(new TCollectOperation(std::get<0>(collectGenStep), std::get<1>(collectGenStep),
        {} /* keep */, {} /* doNoKeep */, {} /* trashGoingToCollect */, storedGenStep < collectGenStep /* advanceBarrier */));
    if (collectGenStep == THelpers::TGenerationStep(ExecutorGeneration, NextLogoBlobStep)) {
        Step();
    }

    StartCollectingIfPossible(ctx);
}

void TKeyValueState::CleanupEmptyTrashBins(const TActorContext &ctx) {
    std::optional<ui64> maxEmptyTrashBins;

    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC239, "CleanupEmptyTrashBins",
        (TabletId, TabletId),
        (TrashBinsForCleanup, TrashForCleanup.size()),
        (TrashCount, GetTrashCount()),
        (TrashInFirstBin, (TrashForCleanup.empty() ? "Nothing" : ToString(TrashForCleanup.begin()->second.size()))),
        (TrashInCurrentBin, (Trash.empty() ? "Nothing" : ToString(Trash.size()))));
    while (!TrashForCleanup.empty() && TrashForCleanup.begin()->second.empty()) {
        maxEmptyTrashBins = TrashForCleanup.begin()->first;
        TrashForCleanup.erase(TrashForCleanup.begin());
    }
    if (maxEmptyTrashBins) {
        CompletedCleanupTrashGeneration = *maxEmptyTrashBins;
        ctx.Send(ctx.SelfID, new TEvKeyValue::TEvForceTabletDataCleanup(*maxEmptyTrashBins));
    }
}

bool TKeyValueState::RemoveCollectedTrash(ISimpleDb &db) {
    if (auto& trash = CollectOperation->TrashGoingToCollect) {
        ui32 collected = 0;

        auto& trashBin = GetCollectingTrashBin();
        for (ui32 maxItemsToStore = 200'000; trash && maxItemsToStore; trash.pop_back(), --maxItemsToStore) {
            const TLogoBlobID& id = trash.back();
            THelpers::DbEraseTrash(id, db);
            ui32 num = trashBin.erase(id);
            Y_ABORT_UNLESS(num == 1);
            TotalTrashSize -= id.BlobSize();
            CountTrashDeleted(id);
            ++collected;
        }

        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC240, "Remove from Trash",
            (TabletId, TabletId), (RemovedCount, collected), (TrashBinSize, trashBin.size()), (TrashBinToCleanup, TrashForCleanup.size()), (TrashCount, GetTrashCount()));

        const TActorContext &ctx = TActivationContext::AsActorContext();
        CleanupEmptyTrashBins(ctx); // trashBin is invalidated by this call

        return trash.empty();
    }

    return true;
}

void TKeyValueState::UpdateStoredState(ISimpleDb &db, const NKeyValue::THelpers::TGenerationStep &genStep)
{
    StoredState.SetCollectGeneration(std::get<0>(genStep));
    StoredState.SetCollectStep(std::get<1>(genStep));
    THelpers::DbUpdateState(StoredState, db);
}

void TKeyValueState::CompleteGCExecute(ISimpleDb &db, const TActorContext &/*ctx*/) {
    if (RemoveCollectedTrash(db)) {
        const ui32 collectGeneration = CollectOperation->Header.GetCollectGeneration();
        const ui32 collectStep = CollectOperation->Header.GetCollectStep();
        auto collectGenStep = THelpers::TGenerationStep(collectGeneration, collectStep);
        UpdateStoredState(db, collectGenStep);
    } else {
        RepeatGCTX = true;
    }
}

void TKeyValueState::CompleteGCComplete(const TActorContext &ctx, const TTabletStorageInfo *info) {
    if (RepeatGCTX) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC20, "Repeat CompleteGC",
            (TabletId, TabletId),
            (TrashBinSize, GetCollectingTrashBin().size()),
            (TrashCount, GetTrashCount()));
        ctx.Send(ctx.SelfID, new TEvKeyValue::TEvCompleteGC(true));
        RepeatGCTX = false;
        return;
    }
    Y_ABORT_UNLESS(CollectOperation);
    CollectOperation.Reset();
    IsCollectEventSent = false;
    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC22, "CompleteGC Complete",
        (TabletId, TabletId),
        (TrashBinSize, GetCollectingTrashBin().size()),
        (TrashCount, GetTrashCount()));
    ProcessPostponedTrims(ctx, info);
    PrepareCollectIfNeeded(ctx);
}

bool TKeyValueState::StartCleanupData(ui64 generation, TActorId sender) {
    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC242, "StartCleanupData",
        (TabletId, TabletId), (generation, generation), (sender, sender));
    const auto &ctx = TActivationContext::AsActorContext();
    if (CompletedCleanupGeneration >= generation) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC243, "StartCleanupData already completed",
            (TabletId, TabletId), (generation, generation), (sender, sender));
        ctx.Send(sender, TEvKeyValue::TEvCleanUpDataResponse::MakeAlreadyCompleted(generation, CompletedCleanupGeneration));
        return false;
    }

    CleanupGenerationToSender[generation].insert(sender);
    if (CompletedCleanupTrashGeneration >= generation) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC244, "StartCleanupData already completed trash generation",
            (TabletId, TabletId), (generation, generation), (sender, sender));
        return false;
    }

    if (TrashForCleanup.empty() || TrashForCleanup.rbegin()->first < generation) {
        auto it = TrashForCleanup.emplace(generation, TSet<TLogoBlobID>()).first;
        it->second.swap(Trash);
    }

    PrepareCollectIfNeeded(TActivationContext::AsActorContext()); // empty trash bins will be cleaned up by PrepareCollectIfNeeded
    return true;
}

void TKeyValueState::ResetCleanupGeneration(const TActorContext &ctx, ui64 generation) {
    for (const auto& [_, trash] : TrashForCleanup) {
        Trash.insert(trash.begin(), trash.end());
    }
    TrashForCleanup.clear();
    for (const auto& [requestedGeneration, recipients] : CleanupGenerationToSender) {
        for (const auto& recipient : recipients) {
            ctx.Send(recipient, TEvKeyValue::TEvCleanUpDataResponse::MakeAborted(requestedGeneration, "Cleanup generation was reset", generation));
        }
    }
    CleanupGenerationToSender.clear();
    CompletedCleanupGeneration = generation;
    CompletedCleanupTrashGeneration = generation;

    CleanupResetGeneration += 1;
}

void TKeyValueState::UpdateCleanupGeneration(ISimpleDb &db, ui64 generation) {
    THelpers::DbUpdateCleanUpGeneration(generation, db);
}

void TKeyValueState::CompleteCleanupDataExecute(ISimpleDb &db, const TActorContext& /*ctx*/, ui64 cleanupGeneration) {
    if (CompletedCleanupGeneration < cleanupGeneration) {
        UpdateCleanupGeneration(db, cleanupGeneration);
    }
}

void TKeyValueState::CompleteCleanupDataComplete(const TActorContext& /*ctx*/, const TTabletStorageInfo* /*info*/, ui64 cleanupGeneration) {
    if (CompletedCleanupGeneration >= cleanupGeneration) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC247, "CompleteCleanupDataComplete nothing to do",
            (CompletedCleanupGeneration, CompletedCleanupGeneration),
            (CompletedCleanupTrashGeneration, CompletedCleanupTrashGeneration),
            (cleanupGeneration, cleanupGeneration));
        return;
    }
    CompletedCleanupGeneration = cleanupGeneration;

    auto maxCleanedGenerationIt = CleanupGenerationToSender.upper_bound(cleanupGeneration);
    if (maxCleanedGenerationIt == CleanupGenerationToSender.begin()) {
        return;
    }
    maxCleanedGenerationIt--;

    while (CleanupGenerationToSender.size() && CleanupGenerationToSender.begin()->first <= CompletedCleanupGeneration) {
        bool last = (maxCleanedGenerationIt == CleanupGenerationToSender.begin());
        auto &[generation, recipients] = *CleanupGenerationToSender.begin();
        for (const auto& sender : recipients) {
            std::unique_ptr<TEvKeyValue::TEvCleanUpDataResponse> response;
            if (last) {
                response = TEvKeyValue::TEvCleanUpDataResponse::MakeSuccess(generation);
            } else {
                response = TEvKeyValue::TEvCleanUpDataResponse::MakeAlreadyCompleted(generation, CompletedCleanupGeneration);
            }
            TActivationContext::AsActorContext().Send(sender, response.release());
        }
        CleanupGenerationToSender.erase(CleanupGenerationToSender.begin());
    }
        
    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC249, "CompleteCleanupDataComplete",
        (CompletedCleanupGeneration, CompletedCleanupGeneration),
        (CompletedCleanupTrashGeneration, CompletedCleanupTrashGeneration),
        (cleanupGeneration, cleanupGeneration));
}

void TKeyValueState::StartGC(const TActorContext &ctx, TVector<TLogoBlobID> &keep, TVector<TLogoBlobID> &doNotKeep,
        TVector<TLogoBlobID>& trashGoingToCollect) {
    // ensure we haven't filled these fields yet
    Y_ABORT_UNLESS(CollectOperation);
    Y_ABORT_UNLESS(!CollectOperation->Keep);
    Y_ABORT_UNLESS(!CollectOperation->DoNotKeep);
    Y_ABORT_UNLESS(!CollectOperation->TrashGoingToCollect);
    // fill in correct values
    CollectOperation->Keep = std::move(keep);
    CollectOperation->DoNotKeep = std::move(doNotKeep);
    CollectOperation->TrashGoingToCollect = std::move(trashGoingToCollect);
    // issue command to collector
    ctx.Send(KeyValueActorId, new TEvKeyValue::TEvCollect());
    Y_ABORT_UNLESS(!IsCollectEventSent);
    IsCollectEventSent = true;
}

void TKeyValueState::StartCollectingIfPossible(const TActorContext &ctx) {
    ALOG_TRACE(NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
            << " IsCollectEventSent# " << IsCollectEventSent << " Marker# KV64");

    // there is nothing to collect yet, or the event was already sent
    Y_ABORT_UNLESS(CollectOperation && !IsCollectEventSent);

    // create generation:step barrier tuple for proposed garbage collection command
    const auto &header = CollectOperation->Header;
    auto collectGenStep = THelpers::TGenerationStep(header.GetCollectGeneration(), header.GetCollectStep());

    // if we have some in flight writes, check if they do not overlap with the new barrier
    if (InFlightForStep) {
        const auto& [step, _] = *InFlightForStep.begin();
        Y_ABORT_UNLESS(collectGenStep < THelpers::TGenerationStep(ExecutorGeneration, step));
    }

    // create stored (previously issued) generation:step barrier as a tuple
    auto storedGenStep = THelpers::TGenerationStep(StoredState.GetCollectGeneration(), StoredState.GetCollectStep());

    // ensure that barrier advances in correct direction
    Y_ABORT_UNLESS(collectGenStep >= storedGenStep);

    // create list of blobs that must have Keep flag
    TVector<TLogoBlobID> keep;
    for (const auto &kv : RefCounts) {
        const TLogoBlobID &id = kv.first;
        const THelpers::TGenerationStep genStep = THelpers::GenerationStep(id);
        if (storedGenStep < genStep && genStep <= collectGenStep) {
            keep.push_back(id);
        }
    }

    // create list of blobs that must have to DoNotKeep flag set; these blobs must have Keep flag written and reside in
    // Trash now
    TVector<TLogoBlobID> doNotKeep;
    TVector<TLogoBlobID> trashGoingToCollect;
    auto &collectingTrashBin = GetCollectingTrashBin();
    doNotKeep.reserve(collectingTrashBin.size());
    trashGoingToCollect.reserve(collectingTrashBin.size());

    for (auto it = collectingTrashBin.begin(); it != collectingTrashBin.end(); ) {
        const TLogoBlobID& id = *it;
        const auto genStep = THelpers::GenerationStep(id);
        if (collectGenStep < genStep) { // we have to advance to next channel in trash
            it = collectingTrashBin.upper_bound(TLogoBlobID(id.TabletID(), Max<ui32>(), Max<ui32>(), id.Channel(),
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode));
            continue;
        }
        if (genStep <= storedGenStep) {
            doNotKeep.push_back(id);
        }
        Y_ABORT_UNLESS(genStep <= collectGenStep);
        trashGoingToCollect.push_back(id);
        ++it;
    }
    doNotKeep.shrink_to_fit();
    trashGoingToCollect.shrink_to_fit();

    Y_ABORT_UNLESS(trashGoingToCollect);

    ALOG_TRACE(NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
            << "Flags Keep.Size# " << keep.size() << " DoNotKeep.Size# " << doNotKeep.size() << " Marker# KV65");

    StartGC(ctx, keep, doNotKeep, trashGoingToCollect);
}

bool TKeyValueState::OnEvCollect(const TActorContext &ctx) {
    Y_ABORT_UNLESS(CollectOperation.Get());
    LastCollectStartedAt = ctx.Now();
    return !CollectOperation->AdvanceBarrier || PerGenerationCounter != Max<ui32>();
}

void TKeyValueState::OnEvCollectDone(const TActorContext& /*ctx*/) {
    PerGenerationCounter += CollectOperation->AdvanceBarrier;
}

void TKeyValueState::OnEvCompleteGC(bool repeat) {
    if (!repeat) {
        CountLatencyBsCollect();
        Y_ABORT_UNLESS(CollectOperation);
        for (const auto& id : CollectOperation->TrashGoingToCollect) {
            CountTrashCollected(id);
        }
    }
}

} // NKeyValue
} // NKikimr
