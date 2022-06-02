#include "keyvalue_state.h"

namespace NKikimr {
namespace NKeyValue {

void TKeyValueState::PrepareCollectIfNeeded(const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "PrepareCollectIfNeeded KeyValue# " << TabletId << " Marker# KV61");

    if (CollectOperation.Get() || InitialCollectsSent) {
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
    ui64 collectNeededStep = 0;
    if (maxId.Generation() == ExecutorGeneration) {
        collectNeededStep = maxId.Step(); // collect up to this maximum blob
    } else {
        // step 0 by default provides collection of all trash blobs up to maxId as the generation is less than current one
        Y_VERIFY(maxId.Generation() < ExecutorGeneration);
    }

    // Don't let the CollectGeneration step backwards
    if (StoredState.GetCollectGeneration() == ExecutorGeneration && collectNeededStep < StoredState.GetCollectStep()) {
        collectNeededStep = StoredState.GetCollectStep();
    }

    // create basic collect operation with zero keep/doNotKeep flag vectors; they will be calculated just before sending
    CollectOperation.Reset(new TCollectOperation(ExecutorGeneration, collectNeededStep, {} /* keep */, {} /* doNoKeep */));
    if (collectNeededStep == NextLogoBlobStep) {
        // advance to the next step if we are going to collect everything up to current one; otherwise we can keep
        // current step
        Step();
    }

    StartCollectingIfPossible(ctx);
}


void TKeyValueState::UpdateGC(ISimpleDb &db, const TActorContext &ctx, bool updateTrash, bool updateState) {
    if (IsDamaged) {
        return;
    }
    Y_VERIFY(CollectOperation);

    ui64 collectGeneration = CollectOperation->Header.GetCollectGeneration();
    ui64 collectStep = CollectOperation->Header.GetCollectStep();

    if (updateTrash) {
        ui64 storedCollectGeneration = StoredState.GetCollectGeneration();
        ui64 storedCollectStep = StoredState.GetCollectStep();

        for (TLogoBlobID &id: CollectOperation->DoNotKeep) {
            THelpers::DbEraseTrash(id, db, ctx);
            ui32 num = Trash.erase(id);
            Y_VERIFY(num == 1);
            CountTrashCollected(id.BlobSize());
        }

        // remove trash entries that were not marked as 'Keep' before, but which are automatically deleted by this barrier
        // to prevent them from being added to 'DoNotKeep' list after
        for (auto it = Trash.begin(); it != Trash.end(); ) {
            THelpers::TGenerationStep trashGenStep = THelpers::GenerationStep(*it);
            bool afterStoredSoftBarrier = trashGenStep > THelpers::TGenerationStep(storedCollectGeneration, storedCollectStep);
            bool beforeSoftBarrier = trashGenStep <= THelpers::TGenerationStep(collectGeneration, collectStep);
            if (afterStoredSoftBarrier && beforeSoftBarrier) {
                CountTrashCollected(it->BlobSize());
                THelpers::DbEraseTrash(*it, db, ctx);
                it = Trash.erase(it);
            } else {
                ++it;
            }
        }
    }

    if (updateState) {
        StoredState.SetCollectGeneration(collectGeneration);
        StoredState.SetCollectStep(collectStep);
        THelpers::DbUpdateState(StoredState, db, ctx);
    }
}

void TKeyValueState::StoreCollectExecute(ISimpleDb &db, const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "StoreCollectExecute KeyValue# " << TabletId
            << " IsDamaged# " << IsDamaged << " Marker# KV62");

    if (IsDamaged) {
        return;
    }
    Y_VERIFY(CollectOperation.Get());

    // This operation will be executed no matter what
    const ui32 collectGen = CollectOperation->Header.GetCollectGeneration();
    const ui32 collectStep = CollectOperation->Header.GetCollectStep();
    THelpers::DbUpdateCollect(collectGen,
        collectStep,
        CollectOperation->Keep,
        CollectOperation->DoNotKeep,
        db, ctx);

    UpdateGC(db, ctx, true, false);
}

void TKeyValueState::StoreCollectComplete(const TActorContext &ctx) {
    ctx.Send(KeyValueActorId, new TEvKeyValue::TEvCollect());
}

void TKeyValueState::EraseCollectExecute(ISimpleDb &db, const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "EraseCollectExecute KeyValue# " << TabletId
            << " IsDamaged# " << IsDamaged << " Marker# KV63");
    if (IsDamaged) {
        return;
    }
    Y_VERIFY(CollectOperation);
    // Erase the collect operation
    THelpers::DbEraseCollect(db, ctx);
    // Update the state
    UpdateGC(db, ctx, false, true);
}

void TKeyValueState::EraseCollectComplete(const TActorContext &ctx) {
    Y_VERIFY(CollectOperation);
    CollectOperation.Reset(nullptr);
    IsCollectEventSent = false;

    // Start new collect operation if needed
    PrepareCollectIfNeeded(ctx);
}

void TKeyValueState::CompleteGCExecute(ISimpleDb &db, const TActorContext &ctx) {
    UpdateGC(db, ctx, true, true);
}

void TKeyValueState::CompleteGCComplete(const TActorContext &ctx) {
    Y_VERIFY(CollectOperation);
    CollectOperation.Reset(nullptr);
    IsCollectEventSent = false;
    PrepareCollectIfNeeded(ctx);
}

// Prepare the completely new full collect operation with the same gen/step, but with correct keep & doNotKeep lists
void TKeyValueState::SendStoreCollect(const TActorContext &ctx, const THelpers::TGenerationStep &genStep,
        TVector<TLogoBlobID> &keep, TVector<TLogoBlobID> &doNotKeep) {
    ui32 generation, step;
    std::tie(generation, step) = genStep;
    CollectOperation.Reset(new TCollectOperation(generation, step, std::move(keep), std::move(doNotKeep)));
    ctx.Send(KeyValueActorId, new TEvKeyValue::TEvStoreCollect());
}

void TKeyValueState::StartGC(const TActorContext &ctx, const THelpers::TGenerationStep &genStep,
        TVector<TLogoBlobID> &keep, TVector<TLogoBlobID> &doNotKeep)
{
    ui32 generation, step;
    std::tie(generation, step) = genStep;
    CollectOperation.Reset(new TCollectOperation(generation, step, std::move(keep), std::move(doNotKeep)));
    ctx.Send(KeyValueActorId, new TEvKeyValue::TEvCollect());
}

void TKeyValueState::StartCollectingIfPossible(const TActorContext &ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
            << " IsCollectEventSent# " << IsCollectEventSent << " Marker# KV64");

    // there is nothing to collect yet, or the event was already sent
    if (!CollectOperation || IsCollectEventSent) {
        return;
    }

    // create generation:step barrier tuple for proposed garbage collection command
    const auto &header = CollectOperation->Header;
    auto collectGenStep = THelpers::TGenerationStep(header.GetCollectGeneration(), header.GetCollectStep());

    // if we have some in flight writes, check if they do not overlap with the new barrier
    if (InFlightForStep) {
        // get the first item from the map with the least value; its key contains step of blob being written
        auto leastWriteIt = InFlightForStep.begin();
        // construct generation:step pair of the oldest in flight blob
        auto inFlightGenStep = THelpers::TGenerationStep(ExecutorGeneration, leastWriteIt->first);
        // check if the new barrier would delete blob being written, in this case hold with the collect operation
        if (inFlightGenStep <= collectGenStep) {
            return;
        }
    }

    Y_VERIFY(!IsCollectEventSent);
    IsCollectEventSent = true;

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
    doNotKeep.reserve(Trash.size());
    for (const TLogoBlobID &id : Trash) {
        if (THelpers::GenerationStep(id) <= storedGenStep) {
            doNotKeep.push_back(id);
        }
    }
    doNotKeep.shrink_to_fit();

    LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "StartCollectingIfPossible KeyValue# " << TabletId
            << "Flags Keep.Size# " << keep.size() << " DoNotKeep.Size# " << doNotKeep.size() << " Marker# KV65");

    StartGC(ctx, collectGenStep, keep, doNotKeep);
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

void TKeyValueState::OnEvCollectDone(ui64 perGenerationCounterStepSize, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    Y_VERIFY(perGenerationCounterStepSize >= 1);
    PerGenerationCounter += perGenerationCounterStepSize;
}

void TKeyValueState::OnEvEraseCollect(const TActorContext &ctx) {
    Y_UNUSED(ctx);
    CountLatencyBsCollect();
}

void TKeyValueState::OnEvCompleteGC() {
    CountLatencyBsCollect();
}

} // NKeyValue
} // NKikimr

