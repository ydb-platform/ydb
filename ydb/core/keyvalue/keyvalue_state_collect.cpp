#include "keyvalue_state.h"
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

void TKeyValueState::PrepareCollectIfNeeded(const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "PrepareCollectIfNeeded KeyValue# " << TabletId << " Marker# KV61");

    if (CmdTrimLeakedBlobsUids || IsCollectEventSent || Trash.empty()) { // can't start GC right now
        return;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // calculate maximum blob id in trash
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    const TLogoBlobID minTrashId = *Trash.begin();
    const TLogoBlobID maxTrashId = *--Trash.end();
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

bool TKeyValueState::RemoveCollectedTrash(ISimpleDb &db, const TActorContext &ctx) {
    if (auto& trash = CollectOperation->TrashGoingToCollect) {
        ui32 collected = 0;

        for (ui32 maxItemsToStore = 200'000; trash && maxItemsToStore; trash.pop_back(), --maxItemsToStore) {
            const TLogoBlobID& id = trash.back();
            THelpers::DbEraseTrash(id, db, ctx);
            ui32 num = Trash.erase(id);
            Y_ABORT_UNLESS(num == 1);
            TotalTrashSize -= id.BlobSize();
            CountTrashDeleted(id);
            ++collected;
        }

        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC24, "Remove from Trash",
            (TabletId, TabletId), (RemovedCount, collected), (TrashCount, Trash.size()));

        return trash.empty();
    }

    return true;
}

void TKeyValueState::UpdateStoredState(ISimpleDb &db, const TActorContext &ctx,
        const NKeyValue::THelpers::TGenerationStep &genStep)
{
    StoredState.SetCollectGeneration(std::get<0>(genStep));
    StoredState.SetCollectStep(std::get<1>(genStep));
    THelpers::DbUpdateState(StoredState, db, ctx);
}

void TKeyValueState::CompleteGCExecute(ISimpleDb &db, const TActorContext &ctx) {
    if (RemoveCollectedTrash(db, ctx)) {
        const ui32 collectGeneration = CollectOperation->Header.GetCollectGeneration();
        const ui32 collectStep = CollectOperation->Header.GetCollectStep();
        auto collectGenStep = THelpers::TGenerationStep(collectGeneration, collectStep);
        UpdateStoredState(db, ctx, collectGenStep);
    } else {
        RepeatGCTX = true;
    }
}

void TKeyValueState::CompleteGCComplete(const TActorContext &ctx, const TTabletStorageInfo *info) {
    if (RepeatGCTX) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC20, "Repeat CompleteGC",
            (TabletId, TabletId),
            (TrashCount, Trash.size()));
        ctx.Send(ctx.SelfID, new TEvKeyValue::TEvCompleteGC(true));
        RepeatGCTX = false;
        return;
    }
    Y_ABORT_UNLESS(CollectOperation);
    CollectOperation.Reset();
    IsCollectEventSent = false;
    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC22, "CompleteGC Complete",
        (TabletId, TabletId),
        (TrashCount, Trash.size()));
    ProcessPostponedTrims(ctx, info);
    PrepareCollectIfNeeded(ctx);
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
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
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
    doNotKeep.reserve(Trash.size());
    trashGoingToCollect.reserve(Trash.size());

    for (auto it = Trash.begin(); it != Trash.end(); ) {
        const TLogoBlobID& id = *it;
        const auto genStep = THelpers::GenerationStep(id);
        if (collectGenStep < genStep) { // we have to advance to next channel in trash
            it = Trash.upper_bound(TLogoBlobID(id.TabletID(), Max<ui32>(), Max<ui32>(), id.Channel(),
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

    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
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
