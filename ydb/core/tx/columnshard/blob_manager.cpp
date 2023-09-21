#include "defs.h"
#include "columnshard_impl.h"
#include "blob_manager.h"
#include "blob_cache.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

#include <ydb/core/base/blobstorage.h>
#include "blobs_action/bs/gc.h"

namespace NKikimr::NColumnShard {

TLogoBlobID ParseLogoBlobId(TString blobId) {
    TLogoBlobID logoBlobId;
    TString err;
    if (!TLogoBlobID::Parse(logoBlobId, blobId, err)) {
        Y_FAIL("%s", err.c_str());
    }
    return logoBlobId;
}

struct TBlobBatch::TBatchInfo : TNonCopyable {
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    TAllocatedGenStepConstPtr GenStepRef;
    const TBlobsManagerCounters Counters;
    const ui32 Gen;
    const ui32 Step;
    const ui32 Channel;

    std::vector<ui32> BlobSizes;
    std::vector<bool> InFlight;
    i32 InFlightCount;
    ui64 TotalSizeBytes;

    TBatchInfo(TIntrusivePtr<TTabletStorageInfo> tabletInfo, TAllocatedGenStepConstPtr genStep, ui32 channel, const TBlobsManagerCounters& counters)
        : TabletInfo(tabletInfo)
        , GenStepRef(genStep)
        , Counters(counters)
        , Gen(std::get<0>(GenStepRef->GenStep))
        , Step(std::get<1>(GenStepRef->GenStep))
        , Channel(channel)
        , InFlightCount(0)
        , TotalSizeBytes(0) {
    }

    TUnifiedBlobId NextBlobId(ui32 blobSize) {
        BlobSizes.push_back(blobSize);
        InFlight.push_back(true);
        ++InFlightCount;
        TotalSizeBytes += blobSize;
        return MakeBlobId(BlobSizes.size() - 1);
    }

    TUnifiedBlobId MakeBlobId(ui32 i) const {
        Y_VERIFY(i < BlobSizes.size());
        const ui32 dsGroup = TabletInfo->GroupFor(Channel, Gen);
        return TUnifiedBlobId(dsGroup, TLogoBlobID(TabletInfo->TabletID, Gen, Step, Channel, BlobSizes[i], i));
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
    Y_VERIFY(blobData.size() <= TLimits::GetBlobSizeLimit(), "Blob %" PRISZT" size exceeds the limit %" PRIu64, blobData.size(), TLimits::GetBlobSizeLimit());

    const ui32 groupId = blobId.GetDsGroup();
    SendWriteRequest(ctx, groupId, blobId.GetLogoBlobId(), blobData, 0, deadline);
}

void TBlobBatch::OnBlobWriteResult(const TLogoBlobID& blobId, const NKikimrProto::EReplyStatus status) {
    BatchInfo->Counters.OnPutResult(blobId.BlobSize());
    Y_VERIFY(status == NKikimrProto::OK, "The caller must handle unsuccessful status");
    Y_VERIFY(BatchInfo);
    Y_VERIFY(BatchInfo->InFlight[blobId.Cookie()], "Blob %s is already acked!", blobId.ToString().c_str());

    BatchInfo->InFlight[blobId.Cookie()] = false;
    --BatchInfo->InFlightCount;
    Y_VERIFY(BatchInfo->InFlightCount >= 0);
}

bool TBlobBatch::AllBlobWritesCompleted() const {
    Y_VERIFY(BatchInfo);
    return BatchInfo->InFlightCount == 0;
}

ui64 TBlobBatch::GetBlobCount() const {
    if (BatchInfo) {
        return BatchInfo->BlobSizes.size();
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

TBlobManager::TBlobManager(TIntrusivePtr<TTabletStorageInfo> tabletInfo, ui32 gen)
    : TabletInfo(tabletInfo)
    , CurrentGen(gen)
    , CurrentStep(0)
    , BlobCountToTriggerGC(BLOB_COUNT_TO_TRIGGER_GC_DEFAULT, 0, Max<i64>())
    , GCIntervalSeconds(GC_INTERVAL_SECONDS_DEFAULT, 0,  Max<i64>())
{}

void TBlobManager::RegisterControls(NKikimr::TControlBoard& icb) {
    icb.RegisterSharedControl(BlobCountToTriggerGC, "ColumnShardControls.BlobCountToTriggerGC");
    icb.RegisterSharedControl(GCIntervalSeconds, "ColumnShardControls.GCIntervalSeconds");
}

bool TBlobManager::LoadState(IBlobManagerDb& db) {
    // Load last collected Generation
    if (!db.LoadLastGcBarrier(LastCollectedGenStep)) {
        return false;
    }

    // Load the keep and delete queues
    std::vector<TUnifiedBlobId> blobsToKeep;
    std::vector<TUnifiedBlobId> blobsToDelete;
    TBlobGroupSelector dsGroupSelector(TabletInfo);
    if (!db.LoadLists(blobsToKeep, blobsToDelete, &dsGroupSelector)) {
        return false;
    }

    for (const auto& unifiedBlobId : blobsToDelete) {
        if (unifiedBlobId.IsDsBlob()) {
            BlobsToDelete.insert(unifiedBlobId.GetLogoBlobId());
            BlobsManagerCounters.OnDeleteBlobMarker(unifiedBlobId.BlobSize());
        } else {
            Y_FAIL("Unexpected blob id: %s", unifiedBlobId.ToStringNew().c_str());
        }
    }
    BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);

    // Build the list of steps that cannot be garbage collected before Keep flag is set on the blobs
    THashSet<TGenStep> genStepsWithBlobsToKeep;
    for (const auto& unifiedBlobId : blobsToKeep) {
        Y_VERIFY(unifiedBlobId.IsDsBlob(), "Not a DS blob id in Keep table: %s", unifiedBlobId.ToStringNew().c_str());

        TLogoBlobID blobId = unifiedBlobId.GetLogoBlobId();
        TGenStep genStep{blobId.Generation(), blobId.Step()};
        Y_VERIFY(genStep > LastCollectedGenStep);

        BlobsToKeep.insert(blobId);
        BlobsManagerCounters.OnKeepMarker(blobId.BlobSize());

        // Keep + DontKeep (probably in different gen:steps)
        // GC could go through it to a greater LastCollectedGenStep
        if (BlobsToDelete.contains(blobId)) {
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

TGenStep TBlobManager::FindNewGCBarrier() {
    TGenStep newCollectGenStep = LastCollectedGenStep;
    size_t numFinished = 0;
    for (auto& allocated : AllocatedGenSteps) {
        if (!allocated->Finished()) {
            break;
        }

        ++numFinished;
        newCollectGenStep = allocated->GenStep;
        Y_VERIFY(newCollectGenStep > CollectGenStepInFlight);
    }
    if (numFinished) {
        AllocatedGenSteps.erase(AllocatedGenSteps.begin(), AllocatedGenSteps.begin() + numFinished);
    }

    if (AllocatedGenSteps.empty()) {
        newCollectGenStep = TGenStep{CurrentGen, CurrentStep};
    }
    return newCollectGenStep;
}

std::shared_ptr<NOlap::NBlobOperations::NBlobStorage::TGCTask> TBlobManager::BuildGCTask(const TString& storageId, const std::shared_ptr<TBlobManager>& manager) {
    if (BlobsToKeep.empty() && BlobsToDelete.empty() && LastCollectedGenStep == TGenStep{CurrentGen, CurrentStep}) {
        ACFL_DEBUG("event", "TBlobManager::NeedStorageGC skip");
        return nullptr;
    }

    TGenStep newCollectGenStep = FindNewGCBarrier();
    Y_VERIFY(newCollectGenStep >= LastCollectedGenStep);

    PreviousGCTime = AppData()->TimeProvider->Now();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "PreparePerGroupGCRequests")("gen", std::get<0>(newCollectGenStep))("step", std::get<1>(newCollectGenStep));
    BlobsManagerCounters.OnNewCollectStep(std::get<0>(newCollectGenStep), std::get<1>(newCollectGenStep));
    const ui32 channelIdx = BLOB_CHANNEL;

    NOlap::NBlobOperations::NBlobStorage::TGCTask::TGCListsByGroup perGroupGCListsInFlight;

    // Clear all possibly not keeped trash in channel's groups: create an event for each group
    if (FirstGC) {
        FirstGC = false;

        // TODO: we need only actual channel history here
        const auto& channelHistory = TabletInfo->ChannelInfo(channelIdx)->History;

        for (auto it = channelHistory.begin(); it != channelHistory.end(); ++it) {
            perGroupGCListsInFlight[it->GroupID];
        }
    }

    // Make per-group Keep/DontKeep lists
    std::deque<TUnifiedBlobId> keepsToErase;
    std::deque<TUnifiedBlobId> deletesToErase;
    {
        // Add all blobs to keep
        auto keepBlobIt = BlobsToKeep.begin();
        for (; keepBlobIt != BlobsToKeep.end(); ++keepBlobIt) {
            TGenStep genStep{keepBlobIt->Generation(), keepBlobIt->Step()};
            if (genStep > newCollectGenStep) {
                break;
            }
            ui32 blobGroup = TabletInfo->GroupFor(keepBlobIt->Channel(), keepBlobIt->Generation());
            perGroupGCListsInFlight[blobGroup].KeepList.insert(*keepBlobIt);
        }
        BlobsToKeep.erase(BlobsToKeep.begin(), keepBlobIt);
        BlobsManagerCounters.OnBlobsKeep(BlobsToKeep);

        // Add all blobs to delete
        auto blobIt = BlobsToDelete.begin();
        for (; blobIt != BlobsToDelete.end(); ++blobIt) {
            TGenStep genStep{blobIt->Generation(), blobIt->Step()};
            if (genStep > newCollectGenStep) {
                break;
            }
            ui32 blobGroup = TabletInfo->GroupFor(blobIt->Channel(), blobIt->Generation());
            NOlap::NBlobOperations::NBlobStorage::TGCTask::TGCLists& gl = perGroupGCListsInFlight[blobGroup];
            bool skipDontKeep = false;
            if (gl.KeepList.erase(*blobIt)) {
                // Skipped blobs still need to be deleted from BlobsToKeep table
                keepsToErase.emplace_back(TUnifiedBlobId(blobGroup, *blobIt));

                if (CurrentGen == blobIt->Generation()) {
                    // If this blob was created and deleted in the current generation then
                    // we can skip sending both Keep and DontKeep flags.
                    // NOTE: its not safe to do this for older generations because there is
                    // a scenario when Keep flag was sent in the old generation and then tablet restarted
                    // before getting the result and removing the blob from the Keep list.
                    skipDontKeep = true;
                    deletesToErase.emplace_back(TUnifiedBlobId(blobGroup, *blobIt));
                    ++CountersUpdate.BlobSkippedEntries;
                }
            }
            if (!skipDontKeep) {
                BlobsManagerCounters.OnCollectDropExplicit(blobIt->BlobSize());
                gl.DontKeepList.insert(*blobIt);
            } else {
                BlobsManagerCounters.OnCollectDropImplicit(blobIt->BlobSize());
            }
        }
        BlobsToDelete.erase(BlobsToDelete.begin(), blobIt);
        BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
    }

    CollectGenStepInFlight = newCollectGenStep;
    return std::make_shared<NOlap::NBlobOperations::NBlobStorage::TGCTask>(storageId, std::move(perGroupGCListsInFlight), newCollectGenStep, std::move(keepsToErase), std::move(deletesToErase), manager);
}

TBlobBatch TBlobManager::StartBlobBatch(ui32 channel) {
    ++CountersUpdate.BatchesStarted;
    Y_VERIFY(channel == BLOB_CHANNEL, "Support for mutiple blob channels is not implemented yet");
    ++CurrentStep;
    TAllocatedGenStepConstPtr genStepRef = new TAllocatedGenStep({CurrentGen, CurrentStep});
    AllocatedGenSteps.push_back(genStepRef);
    auto batchInfo = std::make_unique<TBlobBatch::TBatchInfo>(TabletInfo, genStepRef, channel, BlobsManagerCounters);
    return TBlobBatch(std::move(batchInfo));
}

void TBlobManager::DoSaveBlobBatch(TBlobBatch&& blobBatch, IBlobManagerDb& db) {
    Y_VERIFY(blobBatch.BatchInfo);
    ++CountersUpdate.BatchesCommitted;
    CountersUpdate.BlobsWritten += blobBatch.GetBlobCount();

    LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID
        << " Save Batch GenStep: " << blobBatch.BatchInfo->Gen << ":" << blobBatch.BatchInfo->Step
        << " Blob count: " << blobBatch.BatchInfo->BlobSizes.size());

    // Add this batch to KeepQueue
    TGenStep edgeGenStep = EdgeGenStep();
    for (ui32 i = 0; i < blobBatch.BatchInfo->BlobSizes.size(); ++i) {
        const TUnifiedBlobId blobId = blobBatch.BatchInfo->MakeBlobId(i);
        Y_VERIFY_DEBUG(blobId.IsDsBlob(), "Not a DS blob id: %s", blobId.ToStringNew().c_str());

        auto logoblobId = blobId.GetLogoBlobId();
        TGenStep genStep{logoblobId.Generation(), logoblobId.Step()};

        AFL_VERIFY(genStep > edgeGenStep)("gen_step", genStep)("edge_gen_step", edgeGenStep)("blob_id", blobId.ToStringNew());

        BlobsManagerCounters.OnKeepMarker(logoblobId.BlobSize());
        BlobsToKeep.insert(std::move(logoblobId));
        db.AddBlobToKeep(blobId);
    }
    BlobsManagerCounters.OnBlobsKeep(BlobsToKeep);

    blobBatch.BatchInfo->GenStepRef.Reset();
}

void TBlobManager::DeleteBlob(const TUnifiedBlobId& blobId, IBlobManagerDb& db) {
    ++CountersUpdate.BlobsDeleted;

    // Persist deletion intent
    db.AddBlobToDelete(blobId);

    // Check if the deletion needs to be delayed until the blob is no longer
    // used by in-flight requests
    if (!IsBlobInUsage(blobId)) {
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delete Blob " << blobId);
        TLogoBlobID logoBlobId = blobId.GetLogoBlobId();
        if (BlobsToDelete.emplace(logoBlobId).second) {
            BlobsManagerCounters.OnDeleteBlobMarker(blobId.BlobSize());
            BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
        }
    } else {
        BlobsManagerCounters.OnDeleteBlobDelayedMarker(blobId.BlobSize());
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delay Delete Blob " << blobId);
        BlobsToDeleteDelayed.insert(blobId.GetLogoBlobId());
    }
}

void TBlobManager::OnGCFinished(const TGenStep& genStep, IBlobManagerDb& db) {
    LastCollectedGenStep = genStep;
    db.SaveLastGcBarrier(LastCollectedGenStep);
    CollectGenStepInFlight.reset();
}

void TBlobManager::OnBlobFree(const TUnifiedBlobId& blobId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "blob_free")("blob_id", blobId);
    // Check if the blob is marked for delayed deletion
    const TLogoBlobID logoBlobId = blobId.GetLogoBlobId();
    if (BlobsToDeleteDelayed.erase(logoBlobId)) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("blob_id", blobId)("event", "blob_delayed_deleted");
        BlobsToDelete.insert(logoBlobId);
        BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
        BlobsManagerCounters.OnDeleteBlobMarker(logoBlobId.BlobSize());
    }
}

}
