#include "blob_manager.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

#include <ydb/core/base/blobstorage.h>
#include "gc.h"

namespace NKikimr::NOlap {

TLogoBlobID ParseLogoBlobId(TString blobId) {
    TLogoBlobID logoBlobId;
    TString err;
    if (!TLogoBlobID::Parse(logoBlobId, blobId, err)) {
        Y_ABORT("%s", err.c_str());
    }
    return logoBlobId;
}

struct TBlobBatch::TBatchInfo : TNonCopyable {
private:
    std::vector<TUnifiedBlobId> BlobIds;
public:
    const std::vector<TUnifiedBlobId>& GetBlobIds() const {
        return BlobIds;
    }

    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    TAllocatedGenStepConstPtr GenStepRef;
    const NColumnShard::TBlobsManagerCounters Counters;
    const ui32 Gen;
    const ui32 Step;
    const ui32 Channel;

    std::vector<bool> InFlight;
    i32 InFlightCount;
    ui64 TotalSizeBytes;

    TBatchInfo(TIntrusivePtr<TTabletStorageInfo> tabletInfo, TAllocatedGenStepConstPtr genStep, ui32 channel, const NColumnShard::TBlobsManagerCounters& counters)
        : TabletInfo(tabletInfo)
        , GenStepRef(genStep)
        , Counters(counters)
        , Gen(std::get<0>(GenStepRef->GenStep))
        , Step(std::get<1>(GenStepRef->GenStep))
        , Channel(channel)
        , InFlightCount(0)
        , TotalSizeBytes(0) {
    }

    TUnifiedBlobId NextBlobId(const ui32 blobSize) {
        InFlight.push_back(true);
        ++InFlightCount;
        TotalSizeBytes += blobSize;

        const ui32 dsGroup = TabletInfo->GroupFor(Channel, Gen);
        TUnifiedBlobId nextBlobId(dsGroup, TLogoBlobID(TabletInfo->TabletID, Gen, Step, Channel, blobSize, BlobIds.size()));
        BlobIds.emplace_back(std::move(nextBlobId));
        return BlobIds.back();
    }
};

TBlobBatch::TBlobBatch(std::unique_ptr<TBatchInfo> batchInfo)
    : BatchInfo(std::move(batchInfo))
{}

TBlobBatch::TBlobBatch() = default;
TBlobBatch::TBlobBatch(TBlobBatch&& other) = default;
TBlobBatch& TBlobBatch::operator =(TBlobBatch&& other) = default;
TBlobBatch::~TBlobBatch() = default;

void TBlobBatch::SendWriteRequest(const TActorContext& ctx, ui32 groupId, const TLogoBlobID& logoBlobId, const TString& data, ui64 cookie, TInstant deadline) {
    LOG_S_TRACE("EvPut " << data.size() << " bytes to group " << groupId
        << " at tablet " << BatchInfo->TabletInfo->TabletID);

    auto handleClass = NKikimrBlobStorage::UserData;
    //auto handleClass = NKikimrBlobStorage::AsyncBlob; // TODO: what's the difference?
    auto tactic = TEvBlobStorage::TEvPut::TacticMaxThroughput;

    THolder<TEvBlobStorage::TEvPut> put(
        new TEvBlobStorage::TEvPut(logoBlobId, data, deadline, handleClass, tactic));
    SendPutToGroup(ctx, groupId, BatchInfo->TabletInfo.Get(), std::move(put), cookie);
}

void TBlobBatch::SendWriteBlobRequest(const TString& blobData, const TUnifiedBlobId& blobId, TInstant deadline, const TActorContext& ctx) {
    Y_ABORT_UNLESS(blobData.size() <= NColumnShard::TLimits::GetBlobSizeLimit(), "Blob %" PRISZT" size exceeds the limit %" PRIu64, blobData.size(), NColumnShard::TLimits::GetBlobSizeLimit());

    const ui32 groupId = blobId.GetDsGroup();
    SendWriteRequest(ctx, groupId, blobId.GetLogoBlobId(), blobData, 0, deadline);
}

void TBlobBatch::OnBlobWriteResult(const TLogoBlobID& blobId, const NKikimrProto::EReplyStatus status) {
    BatchInfo->Counters.OnPutResult(blobId.BlobSize());
    Y_ABORT_UNLESS(status == NKikimrProto::OK, "The caller must handle unsuccessful status");
    Y_ABORT_UNLESS(BatchInfo);
    Y_ABORT_UNLESS(blobId.Cookie() < BatchInfo->InFlight.size());
    Y_ABORT_UNLESS(BatchInfo->InFlight[blobId.Cookie()], "Blob %s is already acked!", blobId.ToString().c_str());

    BatchInfo->InFlight[blobId.Cookie()] = false;
    --BatchInfo->InFlightCount;
    Y_ABORT_UNLESS(BatchInfo->InFlightCount >= 0);
}

bool TBlobBatch::AllBlobWritesCompleted() const {
    Y_ABORT_UNLESS(BatchInfo);
    return BatchInfo->InFlightCount == 0;
}

ui64 TBlobBatch::GetBlobCount() const {
    if (BatchInfo) {
        return BatchInfo->GetBlobIds().size();
    }
    return 0;
}

ui64 TBlobBatch::GetTotalSize() const {
    if (BatchInfo) {
        return BatchInfo->TotalSizeBytes;
    }
    return 0;
}


TUnifiedBlobId TBlobBatch::AllocateNextBlobId(const TString& blobData) {
    return BatchInfo->NextBlobId(blobData.size());
}

TBlobManager::TBlobManager(TIntrusivePtr<TTabletStorageInfo> tabletInfo, ui32 gen, const TTabletId selfTabletId)
    : SelfTabletId(selfTabletId)
    , TabletInfo(tabletInfo)
    , CurrentGen(gen)
    , CurrentStep(0)
    , BlobCountToTriggerGC(BLOB_COUNT_TO_TRIGGER_GC_DEFAULT, 0, Max<i64>())
    , GCIntervalSeconds(GC_INTERVAL_SECONDS_DEFAULT, 0,  Max<i64>())
{}

void TBlobManager::RegisterControls(NKikimr::TControlBoard& icb) {
    icb.RegisterSharedControl(BlobCountToTriggerGC, "ColumnShardControls.BlobCountToTriggerGC");
    icb.RegisterSharedControl(GCIntervalSeconds, "ColumnShardControls.GCIntervalSeconds");
}

bool TBlobManager::LoadState(IBlobManagerDb& db, const TTabletId selfTabletId) {
    // Load last collected Generation
    if (!db.LoadLastGcBarrier(LastCollectedGenStep)) {
        return false;
    }

    // Load the keep and delete queues
    std::vector<TUnifiedBlobId> blobsToKeep;
    NColumnShard::TBlobGroupSelector dsGroupSelector(TabletInfo);
    if (!db.LoadLists(blobsToKeep, BlobsToDelete, &dsGroupSelector, selfTabletId)) {
        return false;
    }

    for (auto it = BlobsToDelete.GetIterator(); it.IsValid(); ++it) {
        BlobsManagerCounters.OnDeleteBlobMarker(it.GetBlobId().BlobSize());
    }
    BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);

    // Build the list of steps that cannot be garbage collected before Keep flag is set on the blobs
    THashSet<TGenStep> genStepsWithBlobsToKeep;
    for (const auto& unifiedBlobId : blobsToKeep) {
        TLogoBlobID blobId = unifiedBlobId.GetLogoBlobId();
        TGenStep genStep{blobId.Generation(), blobId.Step()};
        Y_ABORT_UNLESS(genStep > LastCollectedGenStep);

        BlobsToKeep.insert(blobId);
        BlobsManagerCounters.OnKeepMarker(blobId.BlobSize());
        const ui64 groupId = dsGroupSelector.GetGroup(blobId);
        // Keep + DontKeep (probably in different gen:steps)
        // GC could go through it to a greater LastCollectedGenStep
        if (BlobsToDelete.Contains(SelfTabletId, TUnifiedBlobId(groupId, blobId))) {
            continue;
        }

        genStepsWithBlobsToKeep.insert(genStep);
    }
    BlobsManagerCounters.OnBlobsKeep(BlobsToKeep);

    AllocatedGenSteps.clear();
    for (const auto& gs : genStepsWithBlobsToKeep) {
        AllocatedGenSteps.push_back(new TAllocatedGenStep(gs));
    }
    AllocatedGenSteps.push_back(new TAllocatedGenStep({CurrentGen, 0}));

    Sort(AllocatedGenSteps.begin(), AllocatedGenSteps.end(), [](const TAllocatedGenStepConstPtr& a, const TAllocatedGenStepConstPtr& b) {
        return a->GenStep < b->GenStep;
    });

    return true;
}

void TBlobManager::PopGCBarriers(const TGenStep gs) {
    while (AllocatedGenSteps.size() && AllocatedGenSteps.front()->GenStep <= gs) {
        AllocatedGenSteps.pop_front();
    }
}

std::vector<TGenStep> TBlobManager::FindNewGCBarriers() {
    AFL_VERIFY(!CollectGenStepInFlight);
    TGenStep newCollectGenStep = LastCollectedGenStep;
    std::vector<TGenStep> result;
    if (AllocatedGenSteps.empty()) {
        return {TGenStep(CurrentGen, CurrentStep)};
    }
    for (auto& allocated : AllocatedGenSteps) {
        AFL_VERIFY(allocated->GenStep > newCollectGenStep);
        if (!allocated->Finished()) {
            break;
        }
        result.emplace_back(allocated->GenStep);
        newCollectGenStep = allocated->GenStep;
    }
    return result;
}

std::shared_ptr<NBlobOperations::NBlobStorage::TGCTask> TBlobManager::BuildGCTask(const TString& storageId,
    const std::shared_ptr<TBlobManager>& manager, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobsInfo,
    const std::shared_ptr<NBlobOperations::TRemoveGCCounters>& counters) noexcept {
    AFL_VERIFY(!CollectGenStepInFlight);
    if (BlobsToKeep.empty() && BlobsToDelete.IsEmpty() && LastCollectedGenStep == TGenStep{CurrentGen, CurrentStep}) {
        ACFL_DEBUG("event", "TBlobManager::BuildGCTask skip")("current_gen", CurrentGen)("current_step", CurrentStep);
        return nullptr;
    }
    std::vector<TGenStep> newCollectGenSteps = FindNewGCBarriers();

    if (newCollectGenSteps.size()) {
        if (AllocatedGenSteps.size()) {
            AFL_VERIFY(newCollectGenSteps.front() > LastCollectedGenStep);
        } else {
            AFL_VERIFY(newCollectGenSteps.front() == LastCollectedGenStep);
        }
    }

    PreviousGCTime = AppData()->TimeProvider->Now();
    const ui32 channelIdx = BLOB_CHANNEL;
    NBlobOperations::NBlobStorage::TGCTask::TGCListsByGroup perGroupGCListsInFlight;
    // Clear all possibly not kept trash in channel's groups: create an event for each group

    static const ui32 blobsGCCountLimit = 500000;

    const auto predShared = [&](const TUnifiedBlobId& id, const THashSet<TTabletId>& /*tabletIds*/) {
        return id.GetLogoBlobId().TabletID() != (ui64)SelfTabletId;
    };

    TTabletsByBlob extractedToRemoveFromDB = BlobsToDelete.ExtractBlobs(predShared, blobsGCCountLimit);
    if (extractedToRemoveFromDB.GetSize() >= blobsGCCountLimit) {
        newCollectGenSteps.clear();
    } else {
        const auto predRemoveOld = [&](const TUnifiedBlobId& id, const THashSet<TTabletId>& /*tabletIds*/) {
            auto logoBlobId = id.GetLogoBlobId();
            TGenStep genStep{logoBlobId.Generation(), logoBlobId.Step()};
            return genStep < LastCollectedGenStep && id.GetLogoBlobId().TabletID() == (ui64)SelfTabletId;
        };

        TTabletsByBlob extractedOld = BlobsToDelete.ExtractBlobs(predRemoveOld, blobsGCCountLimit - extractedToRemoveFromDB.GetSize());
        extractedToRemoveFromDB.Add(extractedOld);
        for (TTabletsByBlob::TIterator itExtractedOld(extractedOld); itExtractedOld.IsValid(); ++itExtractedOld) {
            const TUnifiedBlobId unifiedBlobId = itExtractedOld.GetBlobId();
            auto logoBlobId = unifiedBlobId.GetLogoBlobId();
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("to_delete_gc", logoBlobId);
            NBlobOperations::NBlobStorage::TGCTask::TGCLists& gl = perGroupGCListsInFlight[unifiedBlobId.GetDsGroup()];
            BlobsManagerCounters.OnCollectDropExplicit(logoBlobId.BlobSize());
            gl.DontKeepList.insert(logoBlobId);
        }
    }


    std::deque<TUnifiedBlobId> keepsToErase;
    std::deque<TUnifiedBlobId> deleteIndex = BlobsToDelete.GroupByGenStep();
    for (auto&& newCollectGenStep : newCollectGenSteps) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "PreparePerGroupGCRequests")("gen_step", newCollectGenStep);
        BlobsManagerCounters.OnNewCollectStep(std::get<0>(newCollectGenStep), std::get<1>(newCollectGenStep));

        // Make per-group Keep/DontKeep lists

        {
            // Add all blobs to keep
            auto keepBlobIt = BlobsToKeep.begin();

            for (; keepBlobIt != BlobsToKeep.end();) {
                TGenStep genStep{keepBlobIt->Generation(), keepBlobIt->Step()};
                AFL_VERIFY(genStep > LastCollectedGenStep);
                if (genStep > newCollectGenStep) {
                    break;
                }
                ui32 blobGroup = TabletInfo->GroupFor(keepBlobIt->Channel(), keepBlobIt->Generation());
                perGroupGCListsInFlight[blobGroup].KeepList.insert(*keepBlobIt);
                keepsToErase.emplace_back(TUnifiedBlobId(blobGroup, *keepBlobIt));
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("to_keep_gc", *keepBlobIt);
                keepBlobIt = BlobsToKeep.erase(keepBlobIt);
                if (keepsToErase.size() > blobsGCCountLimit) {
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "a lot of blobs to gc")("to_remove", extractedToRemoveFromDB.GetSize())("keeps_to_erase", keepsToErase.size())("limit", blobsGCCountLimit)("has_border", !!CollectGenStepInFlight)("border", CollectGenStepInFlight.value_or(LastCollectedGenStep));
                    break;
                }
            }
            AFL_VERIFY(!CollectGenStepInFlight || *CollectGenStepInFlight <= newCollectGenStep);
            if (keepsToErase.size() > blobsGCCountLimit) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "a lot of blobs to gc")("to_remove", extractedToRemoveFromDB.GetSize())("keeps_to_erase", keepsToErase.size())("limit", blobsGCCountLimit)("has_border", !!CollectGenStepInFlight)("border", CollectGenStepInFlight.value_or(LastCollectedGenStep));
                break;
            }
            BlobsManagerCounters.OnBlobsKeep(BlobsToKeep);

            TTabletsByBlob extractedSelf;
            {
                while (deleteIndex.size()) {
                    const auto& blobId = deleteIndex.front().GetLogoBlobId();
                    if (newCollectGenStep < TGenStep(blobId.Generation(), blobId.Step())) {
                        break;
                    }
                    BlobsToDelete.ExtractBlobTo(deleteIndex.front(), extractedSelf);
                    if (extractedToRemoveFromDB.GetSize() + extractedSelf.GetSize() > blobsGCCountLimit) {
                        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "a lot of blobs to gc")("to_remove", extractedToRemoveFromDB.GetSize())("keeps_to_erase", keepsToErase.size())("limit", blobsGCCountLimit);
                        break;
                    }
                    deleteIndex.pop_front();
                }

            }

            extractedToRemoveFromDB.Add(extractedSelf);
            for (TTabletsByBlob::TIterator itExtractedSelf(extractedSelf); itExtractedSelf.IsValid(); ++itExtractedSelf) {
                const TUnifiedBlobId unifiedBlobId = itExtractedSelf.GetBlobId();
                auto logoBlobId = unifiedBlobId.GetLogoBlobId();
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("to_delete_gc", logoBlobId);
                NBlobOperations::NBlobStorage::TGCTask::TGCLists& gl = perGroupGCListsInFlight[unifiedBlobId.GetDsGroup()];
                bool skipDontKeep = false;
                if (gl.KeepList.erase(logoBlobId)) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("to_keep_gc_remove", logoBlobId);
                    // Skipped blobs still need to be deleted from BlobsToKeep table
                    if (CurrentGen == logoBlobId.Generation()) {
                        // If this blob was created and deleted in the current generation then
                        // we can skip sending both Keep and DontKeep flags.
                        // NOTE: its not safe to do this for older generations because there is
                        // a scenario when Keep flag was sent in the old generation and then tablet restarted
                        // before getting the result and removing the blob from the Keep list.
                        skipDontKeep = true;
                        ++CountersUpdate.BlobSkippedEntries;
                    }
                }
                if (!skipDontKeep) {
                    BlobsManagerCounters.OnCollectDropExplicit(logoBlobId.BlobSize());
                    gl.DontKeepList.insert(logoBlobId);
                } else {
                    BlobsManagerCounters.OnCollectDropImplicit(logoBlobId.BlobSize());
                }
            }
            BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
        }
        if (std::get<0>(newCollectGenStep) == CurrentGen) {
            CollectGenStepInFlight = newCollectGenStep;
        }
        if (keepsToErase.size() > blobsGCCountLimit) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "a lot of blobs to gc")("to_remove", extractedToRemoveFromDB.GetSize())("keeps_to_erase", keepsToErase.size())("limit", blobsGCCountLimit);
            break;
        }
    }
    if (CollectGenStepInFlight) {
        if (FirstGC) {
            FirstGC = false;

            // TODO: we need only actual channel history here
            const auto& channelHistory = TabletInfo->ChannelInfo(channelIdx)->History;

            for (auto it = channelHistory.begin(); it != channelHistory.end(); ++it) {
                perGroupGCListsInFlight[it->GroupID];
            }
        }
        PopGCBarriers(*CollectGenStepInFlight);
    }
    if (BlobsToKeep.size() && CollectGenStepInFlight) {
        TGenStep genStepFront{BlobsToKeep.begin()->Generation(), BlobsToKeep.begin()->Step()};
        AFL_VERIFY(*CollectGenStepInFlight < genStepFront);
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("notice", "collect_gen_step")("value", CollectGenStepInFlight)("current_gen", CurrentGen);
    auto removeCategories = sharedBlobsInfo->BuildRemoveCategories(std::move(extractedToRemoveFromDB));

    auto result = std::make_shared<NBlobOperations::NBlobStorage::TGCTask>(storageId, std::move(perGroupGCListsInFlight), CollectGenStepInFlight,
        std::move(keepsToErase), manager, std::move(removeCategories), counters, TabletInfo->TabletID, CurrentGen);
    if (result->IsEmpty()) {
        CollectGenStepInFlight = {};
        return nullptr;
    }
    return result;
}



TBlobBatch TBlobManager::StartBlobBatch(ui32 channel) {
    AFL_VERIFY(++CurrentStep < Max<ui32>() - 10);
    ++CountersUpdate.BatchesStarted;
    Y_ABORT_UNLESS(channel == BLOB_CHANNEL, "Support for mutiple blob channels is not implemented yet");
    ++CurrentStep;
    TAllocatedGenStepConstPtr genStepRef = new TAllocatedGenStep({CurrentGen, CurrentStep});
    AllocatedGenSteps.push_back(genStepRef);
    auto batchInfo = std::make_unique<TBlobBatch::TBatchInfo>(TabletInfo, genStepRef, channel, BlobsManagerCounters);
    return TBlobBatch(std::move(batchInfo));
}

void TBlobManager::DoSaveBlobBatch(TBlobBatch&& blobBatch, IBlobManagerDb& db) {
    Y_ABORT_UNLESS(blobBatch.BatchInfo);
    ++CountersUpdate.BatchesCommitted;
    CountersUpdate.BlobsWritten += blobBatch.GetBlobCount();

    LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID
        << " Save Batch GenStep: " << blobBatch.BatchInfo->Gen << ":" << blobBatch.BatchInfo->Step
        << " Blob count: " << blobBatch.BatchInfo->GetBlobIds().size());

    // Add this batch to KeepQueue
    TGenStep edgeGenStep = EdgeGenStep();
    for (auto&& blobId: blobBatch.BatchInfo->GetBlobIds()) {
        auto logoBlobId = blobId.GetLogoBlobId();
        TGenStep genStep{logoBlobId.Generation(), logoBlobId.Step()};

        AFL_VERIFY(genStep > edgeGenStep)("gen_step", genStep)("edge_gen_step", edgeGenStep)("blob_id", blobId.ToStringNew());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("to_keep", logoBlobId.ToString());

        BlobsManagerCounters.OnKeepMarker(logoBlobId.BlobSize());
        BlobsToKeep.insert(std::move(logoBlobId));
        db.AddBlobToKeep(blobId);
    }
    BlobsManagerCounters.OnBlobsKeep(BlobsToKeep);

    blobBatch.BatchInfo->GenStepRef.Reset();
}

void TBlobManager::DeleteBlobOnExecute(const TTabletId tabletId, const TUnifiedBlobId& blobId, IBlobManagerDb& db) {
    // Persist deletion intent
    db.AddBlobToDelete(blobId, tabletId);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("to_delete_on_execute", blobId);
}

void TBlobManager::DeleteBlobOnComplete(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("to_delete_on_complete", blobId)("tablet_id_delete", (ui64)tabletId);
    ++CountersUpdate.BlobsDeleted;

    // Check if the deletion needs to be delayed until the blob is no longer
    // used by in-flight requests
    if (!IsBlobInUsage(blobId)) {
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delete Blob " << blobId);
        Y_UNUSED(BlobsToDelete.Add(tabletId, blobId));
        BlobsManagerCounters.OnDeleteBlobMarker(blobId.BlobSize());
        BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
    } else {
        BlobsManagerCounters.OnDeleteBlobDelayedMarker(blobId.BlobSize());
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delay Delete Blob " << blobId);
        BlobsToDeleteDelayed.Add(tabletId, blobId);
    }
}

void TBlobManager::OnGCFinishedOnExecute(const std::optional<TGenStep>& genStep, IBlobManagerDb& db) {
    if (genStep) {
        db.SaveLastGcBarrier(*genStep);
    }
}

void TBlobManager::OnGCFinishedOnComplete(const std::optional<TGenStep>& genStep) {
    if (genStep) {
        LastCollectedGenStep = *genStep;
        CollectGenStepInFlight.reset();
    } else {
        AFL_VERIFY(!CollectGenStepInFlight);
    }
}

void TBlobManager::OnBlobFree(const TUnifiedBlobId& blobId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "blob_free")("blob_id", blobId);
    // Check if the blob is marked for delayed deletion
    if (BlobsToDeleteDelayed.ExtractBlobTo(blobId, BlobsToDelete)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("blob_id", blobId)("event", "blob_delayed_deleted");
        BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
        BlobsManagerCounters.OnDeleteBlobMarker(blobId.GetLogoBlobId().BlobSize());
    }
}

}
