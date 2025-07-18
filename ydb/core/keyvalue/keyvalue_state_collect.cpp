#include "keyvalue_state.h"
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NKeyValue {

void TKeyValueState::PrepareCollectIfNeeded(const TActorContext &ctx) {
    ALOG_TRACE(NKikimrServices::KEYVALUE, "PrepareCollectIfNeeded KeyValue# " << TabletId << " Marker# KV61");

    VacuumEmptyTrashBins(ctx);
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

void TKeyValueState::VacuumEmptyTrashBins(const TActorContext &ctx) {
    std::optional<ui64> maxEmptyTrashBins;

    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC239, "VacuumEmptyTrashBins",
        (TabletId, TabletId),
        (TrashBinsForVacuum, TrashForVacuum.size()),
        (TrashCount, GetTrashCount()),
        (TrashInFirstBin, (TrashForVacuum.empty() ? "Nothing" : ToString(TrashForVacuum.begin()->second.size()))),
        (TrashInCurrentBin, (Trash.empty() ? "Nothing" : ToString(Trash.size()))));
    while (!TrashForVacuum.empty() && TrashForVacuum.begin()->second.empty()) {
        maxEmptyTrashBins = TrashForVacuum.begin()->first;
        TrashForVacuum.erase(TrashForVacuum.begin());
    }
    if (maxEmptyTrashBins) {
        CompletedVacuumTrashGeneration = *maxEmptyTrashBins;
        ctx.Send(ctx.SelfID, new TEvKeyValue::TEvForceTabletVacuum(*maxEmptyTrashBins));
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
            (TabletId, TabletId), (RemovedCount, collected), (TrashBinSize, trashBin.size()), (TrashBinToVacuum, TrashForVacuum.size()), (TrashCount, GetTrashCount()));

        const TActorContext &ctx = TActivationContext::AsActorContext();
        VacuumEmptyTrashBins(ctx); // trashBin is invalidated by this call

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

bool TKeyValueState::StartVacuum(ui64 generation, TActorId sender) {
    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC242, "StartVacuum",
        (TabletId, TabletId), (generation, generation), (sender, sender));
    const auto &ctx = TActivationContext::AsActorContext();
    if (CompletedVacuumGeneration >= generation) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC243, "StartVacuum already completed",
            (TabletId, TabletId), (generation, generation), (sender, sender));
        ctx.Send(sender, TEvKeyValue::TEvVacuumResponse::MakeAlreadyCompleted(generation, CompletedVacuumGeneration, TabletId));
        return false;
    }

    VacuumGenerationToSender[generation].insert(sender);
    if (CompletedVacuumTrashGeneration >= generation) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC244, "StartVacuum already completed trash generation",
            (TabletId, TabletId), (generation, generation), (sender, sender));
        return false;
    }

    if (TrashForVacuum.empty() || TrashForVacuum.rbegin()->first < generation) {
        auto it = TrashForVacuum.emplace(generation, TSet<TLogoBlobID>()).first;
        it->second.swap(Trash);
    }

    PrepareCollectIfNeeded(TActivationContext::AsActorContext()); // empty trash bins will be cleaned up by PrepareCollectIfNeeded
    return true;
}

void TKeyValueState::ResetVacuumGeneration(const TActorContext &ctx, ui64 generation) {
    for (const auto& [_, trash] : TrashForVacuum) {
        Trash.insert(trash.begin(), trash.end());
    }
    TrashForVacuum.clear();
    for (const auto& [requestedGeneration, recipients] : VacuumGenerationToSender) {
        for (const auto& recipient : recipients) {
            ctx.Send(recipient, TEvKeyValue::TEvVacuumResponse::MakeAborted(requestedGeneration, "Vacuum generation was reset", generation, TabletId));
        }
    }
    VacuumGenerationToSender.clear();
    CompletedVacuumGeneration = generation;
    CompletedVacuumTrashGeneration = generation;

    VacuumResetGeneration += 1;
}

void TKeyValueState::UpdateVacuumGeneration(ISimpleDb &db, ui64 generation) {
    THelpers::DbUpdateVacuumGeneration(generation, db);
}

void TKeyValueState::CompleteVacuumExecute(ISimpleDb &db, const TActorContext& /*ctx*/, ui64 vacuumGeneration) {
    if (CompletedVacuumGeneration < vacuumGeneration) {
        UpdateVacuumGeneration(db, vacuumGeneration);
    }
}

void TKeyValueState::CompleteVacuumComplete(const TActorContext& /*ctx*/, const TTabletStorageInfo* /*info*/, ui64 vacuumGeneration) {
    if (CompletedVacuumGeneration >= vacuumGeneration) {
        STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC247, "CompleteVacuumComplete nothing to do",
            (CompletedVacuumGeneration, CompletedVacuumGeneration),
            (CompletedVacuumTrashGeneration, CompletedVacuumTrashGeneration),
            (vacuumGeneration, vacuumGeneration));
        return;
    }
    CompletedVacuumGeneration = vacuumGeneration;

    auto maxCleanedGenerationIt = VacuumGenerationToSender.upper_bound(vacuumGeneration);
    if (maxCleanedGenerationIt == VacuumGenerationToSender.begin()) {
        return;
    }
    maxCleanedGenerationIt--;

    while (VacuumGenerationToSender.size() && VacuumGenerationToSender.begin()->first <= CompletedVacuumGeneration) {
        bool last = (maxCleanedGenerationIt == VacuumGenerationToSender.begin());
        auto &[generation, recipients] = *VacuumGenerationToSender.begin();
        for (const auto& sender : recipients) {
            std::unique_ptr<TEvKeyValue::TEvVacuumResponse> response;
            if (last) {
                response = TEvKeyValue::TEvVacuumResponse::MakeSuccess(generation, TabletId);
            } else {
                response = TEvKeyValue::TEvVacuumResponse::MakeAlreadyCompleted(generation, CompletedVacuumGeneration, TabletId);
            }
            TActivationContext::AsActorContext().Send(sender, response.release());
        }
        VacuumGenerationToSender.erase(VacuumGenerationToSender.begin());
    }

    STLOG(NLog::PRI_DEBUG, NKikimrServices::KEYVALUE_GC, KVC249, "CompleteVacuumComplete",
        (CompletedVacuumGeneration, CompletedVacuumGeneration),
        (CompletedVacuumTrashGeneration, CompletedVacuumTrashGeneration),
        (vacuumGeneration, vacuumGeneration));
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
