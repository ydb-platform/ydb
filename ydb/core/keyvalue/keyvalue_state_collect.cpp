#include "keyvalue_state.h"
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

void TKeyValueState::PrepareCollectIfNeeded(const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "PrepareCollectIfNeeded KeyValue# " << TabletId << " Marker# KV61");

    if (CmdTrimLeakedBlobsUids) {
        // Do not start garbage collection while we are trimming to avoid race.
        return;
    }

    if (IsCollectEventSent || InitialCollectsSent) {
        // We already are trying to collect something, just pass this time.
        return;
    }

    if (Trash.empty()) {
        // There is nothing to collect.
        return;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // calculate maximum blob id in trash
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    TLogoBlobID maxId = *Trash.rbegin();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // derive new collect step for this operation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    THelpers::TGenerationStep inflightGenStep(Max<ui32>(), Max<ui32>());
    if (InFlightForStep) {
        const auto& [step, _] = *InFlightForStep.begin();
        Y_VERIFY(step);
        inflightGenStep = THelpers::TGenerationStep(ExecutorGeneration, step - 1);
    }
    const auto storedGenStep = THelpers::TGenerationStep(StoredState.GetCollectGeneration(), StoredState.GetCollectStep());
    const auto collectGenStep = Min(inflightGenStep, Max(storedGenStep, THelpers::GenerationStep(maxId)));
    Y_VERIFY(THelpers::TGenerationStep(ExecutorGeneration, 0) <= collectGenStep);
    Y_VERIFY(storedGenStep <= collectGenStep);

    // check if it is useful to start any collection
    const TLogoBlobID minTrashId = *Trash.begin();
    if (collectGenStep < THelpers::GenerationStep(minTrashId)) {
        return; // we do not have the opportunity to collect anything here with this allowed barrier
    }

    // create basic collect operation with zero keep/doNotKeep flag vectors; they will be calculated just before sending
    CollectOperation.Reset(new TCollectOperation(std::get<0>(collectGenStep), std::get<1>(collectGenStep),
        {} /* keep */, {} /* doNoKeep */, {} /* trashGoingToCollect */));
    if (std::get<1>(collectGenStep) == NextLogoBlobStep) {
        // advance to the next step if we are going to collect everything up to current one; otherwise we can keep
        // current step
        Step();
    }

    StartCollectingIfPossible(ctx);
}

bool TKeyValueState::RemoveCollectedTrash(ISimpleDb &db, const TActorContext &ctx) {
    if (auto& trash = CollectOperation->TrashGoingToCollect) {
        ui32 collected = 0;

        for (ui32 maxItemsToStore = 10'000; trash && maxItemsToStore; trash.pop_back(), --maxItemsToStore) {
            const TLogoBlobID& id = trash.back();
            THelpers::DbEraseTrash(id, db, ctx);
            ui32 num = Trash.erase(id);
            Y_VERIFY(num == 1);
            CountTrashCollected(id.BlobSize());
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
        ctx.Send(ctx.SelfID, new TEvKeyValue::TEvCompleteGC());
        RepeatGCTX = false;
        return;
    }
    Y_VERIFY(CollectOperation);
    CollectOperation.Reset(nullptr);
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
    Y_VERIFY(CollectOperation);
    Y_VERIFY(!CollectOperation->Keep);
    Y_VERIFY(!CollectOperation->DoNotKeep);
    Y_VERIFY(!CollectOperation->TrashGoingToCollect);
    // fill in correct values
    CollectOperation->Keep = std::move(keep);
    CollectOperation->DoNotKeep = std::move(doNotKeep);
    CollectOperation->TrashGoingToCollect = std::move(trashGoingToCollect);
    // issue command to collector
    ctx.Send(KeyValueActorId, new TEvKeyValue::TEvCollect());
    Y_VERIFY(!IsCollectEventSent);
    IsCollectEventSent = true;
}

void TKeyValueState::StartCollectingIfPossible(const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
            << " IsCollectEventSent# " << IsCollectEventSent << " Marker# KV64");

    // there is nothing to collect yet, or the event was already sent
    Y_VERIFY(CollectOperation && !IsCollectEventSent);

    // create generation:step barrier tuple for proposed garbage collection command
    const auto &header = CollectOperation->Header;
    auto collectGenStep = THelpers::TGenerationStep(header.GetCollectGeneration(), header.GetCollectStep());

    // if we have some in flight writes, check if they do not overlap with the new barrier
    if (InFlightForStep) {
        const auto& [step, _] = *InFlightForStep.begin();
        Y_VERIFY(collectGenStep < THelpers::TGenerationStep(ExecutorGeneration, step));
    }

    // create stored (previously issued) generation:step barrier as a tuple
    auto storedGenStep = THelpers::TGenerationStep(StoredState.GetCollectGeneration(), StoredState.GetCollectStep());

    // ensure that barrier advances in correct direction
    Y_VERIFY(collectGenStep >= storedGenStep);

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

    for (const TLogoBlobID& id : Trash) {
        auto genStep = THelpers::GenerationStep(id);
        if (collectGenStep < genStep) {
            break;
        }
        if (genStep <= storedGenStep || id.Generation() < ExecutorGeneration) { // assume Keep flag was issued to these blobs
            doNotKeep.push_back(id);
        }
        Y_VERIFY(genStep <= collectGenStep);
        trashGoingToCollect.push_back(id);
    }
    doNotKeep.shrink_to_fit();
    trashGoingToCollect.shrink_to_fit();

    Y_VERIFY(trashGoingToCollect);

    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
            << "Flags Keep.Size# " << keep.size() << " DoNotKeep.Size# " << doNotKeep.size() << " Marker# KV65");

    StartGC(ctx, keep, doNotKeep, trashGoingToCollect);
}

ui64 TKeyValueState::OnEvCollect(const TActorContext &ctx) {
    Y_UNUSED(ctx);
    LastCollectStartedAt = TAppData::TimeProvider->Now();
    Y_VERIFY(CollectOperation.Get());
    ui64 perGenerationCounterStepSize = TEvBlobStorage::TEvCollectGarbage::PerGenerationCounterStepSize(
            &CollectOperation->Keep, &CollectOperation->DoNotKeep);
    ui64 nextPerGenerationCounter = ui64(PerGenerationCounter) + perGenerationCounterStepSize;
    if (nextPerGenerationCounter > ui64(Max<ui32>())) {
        return 0;
    }
    return perGenerationCounterStepSize;
}

void TKeyValueState::OnEvCollectDone(ui64 perGenerationCounterStepSize, TActorId collector, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    Y_VERIFY(perGenerationCounterStepSize >= 1);
    PerGenerationCounter += perGenerationCounterStepSize;
    CollectorActorId = collector;
}

void TKeyValueState::OnEvCompleteGC() {
    CountLatencyBsCollect();
}

} // NKeyValue
} // NKikimr
