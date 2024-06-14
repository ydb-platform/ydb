#include "blob_manager.h"
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

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

struct TBlobBatch::TBatchInfo: TNonCopyable {
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
        , Gen(GenStepRef->GenStep.Generation())
        , Step(GenStepRef->GenStep.Step())
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
    : BatchInfo(std::move(batchInfo)) {
}

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
{
}

void TBlobManager::RegisterControls(NKikimr::TControlBoard& /*icb*/) {
}

bool TBlobManager::LoadState(IBlobManagerDb& db, const TTabletId selfTabletId) {
    // Load last collected Generation
    if (!db.LoadLastGcBarrier(LastCollectedGenStep)) {
        return false;
    }
    if (!db.LoadGCBarrierPreparation(GCBarrierPreparation)) {
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
    std::map<TGenStep, std::set<TLogoBlobID>> blobsToKeepLocal;
    for (const auto& unifiedBlobId : blobsToKeep) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("add_blob_to_keep", unifiedBlobId.ToStringNew());
        TLogoBlobID blobId = unifiedBlobId.GetLogoBlobId();
        TGenStep genStep(blobId);
        Y_ABORT_UNLESS(LastCollectedGenStep < genStep);

        AFL_VERIFY(blobsToKeepLocal[genStep].emplace(blobId).second)("blob_to_keep_double", unifiedBlobId.ToStringNew());
        BlobsManagerCounters.OnKeepMarker(blobId.BlobSize());
        const ui64 groupId = dsGroupSelector.GetGroup(blobId);
        // Keep + DontKeep (probably in different gen:steps)
        // GC could go through it to a greater LastCollectedGenStep
        if (BlobsToDelete.Contains(SelfTabletId, TUnifiedBlobId(groupId, blobId))) {
            continue;
        }

        genStepsWithBlobsToKeep.insert(genStep);
    }
    std::swap(blobsToKeepLocal, BlobsToKeep);
    BlobsManagerCounters.OnBlobsKeep(BlobsToKeep);

    AllocatedGenSteps.clear();
    for (const auto& gs : genStepsWithBlobsToKeep) {
        AllocatedGenSteps.push_back(new TAllocatedGenStep(gs));
    }
    AllocatedGenSteps.push_back(new TAllocatedGenStep({ CurrentGen, 0 }));

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

std::deque<TGenStep> TBlobManager::FindNewGCBarriers() {
    TGenStep newCollectGenStep = LastCollectedGenStep;
    std::deque<TGenStep> result;
    if (AllocatedGenSteps.empty()) {
        result.emplace_back(TGenStep(CurrentGen, CurrentStep));
    }
    for (auto& allocated : AllocatedGenSteps) {
        AFL_VERIFY(allocated->GenStep > newCollectGenStep);
        if (!allocated->Finished()) {
            break;
        }
        result.emplace_back(allocated->GenStep);
        newCollectGenStep = allocated->GenStep;
    }
    if (result.empty() || LastCollectedGenStep < result.front()) {
        result.emplace_front(LastCollectedGenStep);
    }
    return result;
}

class TBlobManager::TGCContext {
private:
    static inline const ui32 BlobsGCCountLimit = 50000;
    YDB_ACCESSOR_DEF(NBlobOperations::NBlobStorage::TGCTask::TGCListsByGroup, PerGroupGCListsInFlight);
    YDB_ACCESSOR_DEF(TTabletsByBlob, ExtractedToRemoveFromDB);
    YDB_ACCESSOR_DEF(std::deque<TUnifiedBlobId>, KeepsToErase);
    YDB_READONLY_DEF(std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>, SharedBlobsManager);
public:
    TGCContext(const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobsManager)
        : SharedBlobsManager(sharedBlobsManager)
    {

    }

    void InitializeFirst(const TIntrusivePtr<TTabletStorageInfo>& tabletInfo) {
        // Clear all possibly not kept trash in channel's groups: create an event for each group
        // TODO: we need only actual channel history here
        for (ui32 channelIdx = 2; channelIdx < tabletInfo->Channels.size(); ++channelIdx) {
            const auto& channelHistory = tabletInfo->ChannelInfo(channelIdx)->History;

            for (auto it = channelHistory.begin(); it != channelHistory.end(); ++it) {
                PerGroupGCListsInFlight[TBlobAddress(it->GroupID, channelIdx)];
            }
        }
    }

    bool IsFull() const {
        return KeepsToErase.size() + ExtractedToRemoveFromDB.GetSize() >= BlobsGCCountLimit;
    }

    ui64 GetFreeSpace() const {
        return IsFull() ? 0 : (BlobsGCCountLimit - ExtractedToRemoveFromDB.GetSize() - KeepsToErase.size());
    }
};

void TBlobManager::DrainDeleteTo(const TGenStep& dest, TGCContext& gcContext) {
    const auto predShared = [&](const TUnifiedBlobId& id, const THashSet<TTabletId>& /*tabletIds*/) {
        if (id.GetLogoBlobId().TabletID() == (ui64)SelfTabletId) {
            auto logoBlobId = id.GetLogoBlobId();
            TGenStep genStep{ logoBlobId.Generation(), logoBlobId.Step() };
            if (dest < genStep) {
                return false;
            }
        }
        return true;
    };
    TTabletsByBlob extractedOld = BlobsToDelete.ExtractBlobs(predShared, gcContext.GetFreeSpace());
    gcContext.MutableExtractedToRemoveFromDB().Add(extractedOld);

    TTabletId tabletId;
    TUnifiedBlobId unifiedBlobId;
    while (extractedOld.ExtractFront(tabletId, unifiedBlobId)) {
        TBlobAddress bAddress(unifiedBlobId.GetDsGroup(), unifiedBlobId.GetLogoBlobId().Channel());
        auto logoBlobId = unifiedBlobId.GetLogoBlobId();
        if (!gcContext.GetSharedBlobsManager()->BuildStoreCategories({ unifiedBlobId }).GetDirect().IsEmpty()) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_delete_gc", unifiedBlobId.ToStringNew());
            NBlobOperations::NBlobStorage::TGCTask::TGCLists& gl = gcContext.MutablePerGroupGCListsInFlight()[bAddress];
            gl.DontKeepList.insert(logoBlobId);
        } else {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_delete_gc", unifiedBlobId.ToStringNew())("skip_reason", "not_direct");
        }
    }
}

void TBlobManager::DrainKeepTo(const TGenStep& dest, TGCContext& gcContext) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "PreparePerGroupGCRequests")("gen_step", dest)("gs_blobs_to_keep_count", BlobsToKeep.size());
    for (; BlobsToKeep.size(); BlobsToKeep.erase(BlobsToKeep.begin())) {
        auto gsBlobs = BlobsToKeep.begin();
        TGenStep genStep = gsBlobs->first;
        AFL_VERIFY(LastCollectedGenStep < genStep)("last", LastCollectedGenStep.ToString())("gen", genStep.ToString());
        if (dest < genStep) {
            break;
        }
        for (auto&& keepBlobIt : gsBlobs->second) {
            const ui32 blobGroup = TabletInfo->GroupFor(keepBlobIt.Channel(), keepBlobIt.Generation());
            TBlobAddress bAddress(blobGroup, keepBlobIt.Channel());
            const TUnifiedBlobId keepUnified(blobGroup, keepBlobIt);
            gcContext.MutableKeepsToErase().emplace_back(keepUnified);
            if (BlobsToDelete.ExtractBlobTo(keepUnified, gcContext.MutableExtractedToRemoveFromDB())) {
                if (keepBlobIt.Generation() == CurrentGen) {
                    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_not_keep", keepUnified.ToStringNew());
                    continue;
                }
                if (gcContext.GetSharedBlobsManager()->BuildStoreCategories({ keepUnified }).GetDirect().IsEmpty()) {
                    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_not_keep_not_direct", keepUnified.ToStringNew());
                    continue;
                }
                AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_not_keep_old", keepUnified.ToStringNew());
                gcContext.MutablePerGroupGCListsInFlight()[bAddress].DontKeepList.insert(keepBlobIt);
            } else {
                AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_keep", keepUnified.ToStringNew());
                gcContext.MutablePerGroupGCListsInFlight()[bAddress].KeepList.insert(keepBlobIt);
            }
        }
    }
}

std::shared_ptr<NBlobOperations::NBlobStorage::TGCTask> TBlobManager::BuildGCTask(const TString& storageId,
    const std::shared_ptr<TBlobManager>& manager, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobsInfo,
    const std::shared_ptr<NBlobOperations::TRemoveGCCounters>& counters) noexcept {
    AFL_VERIFY(!CollectGenStepInFlight);
    if (BlobsToKeep.empty() && BlobsToDelete.IsEmpty() && LastCollectedGenStep == TGenStep{ CurrentGen, CurrentStep }) {
        ACFL_DEBUG("event", "TBlobManager::BuildGCTask skip")("current_gen", CurrentGen)("current_step", CurrentStep);
        return nullptr;
    }

    if (AppData()->TimeProvider->Now() - PreviousGCTime < NYDBTest::TControllers::GetColumnShardController()->GetOverridenGCPeriod(TDuration::Seconds(GC_INTERVAL_SECONDS))) {
        return nullptr;
    }

    PreviousGCTime = AppData()->TimeProvider->Now();
    TGCContext gcContext(sharedBlobsInfo);
    if (FirstGC) {
        gcContext.InitializeFirst(TabletInfo);
        FirstGC = false;
    }

    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("action_id", TGUID::CreateTimebased().AsGuidString());
    const std::deque<TGenStep> newCollectGenSteps = FindNewGCBarriers();
    AFL_VERIFY(newCollectGenSteps.size());
    AFL_VERIFY(newCollectGenSteps.front() == LastCollectedGenStep);

    if (GCBarrierPreparation != LastCollectedGenStep) {
        if (!GCBarrierPreparation.Generation()) {
            for (auto&& newCollectGenStep : newCollectGenSteps) {
                DrainKeepTo(newCollectGenStep, gcContext);
                CollectGenStepInFlight = std::max(CollectGenStepInFlight.value_or(newCollectGenStep), newCollectGenStep);
                if (gcContext.IsFull()) {
                    break;
                }
            }
            DrainDeleteTo(*CollectGenStepInFlight, gcContext);
        } else {
            AFL_VERIFY(GCBarrierPreparation.Generation() != CurrentGen);
            AFL_VERIFY(LastCollectedGenStep <= GCBarrierPreparation);
            CollectGenStepInFlight = GCBarrierPreparation;
            DrainKeepTo(*CollectGenStepInFlight, gcContext);
            DrainDeleteTo(*CollectGenStepInFlight, gcContext);
        }
    }
    if (!gcContext.IsFull()) {
        for (auto&& newCollectGenStep : newCollectGenSteps) {
            DrainKeepTo(newCollectGenStep, gcContext);
            DrainDeleteTo(newCollectGenStep, gcContext);
            CollectGenStepInFlight = std::max(CollectGenStepInFlight.value_or(newCollectGenStep), newCollectGenStep);
            if (gcContext.IsFull()) {
                break;
            }
        }
    }

    AFL_VERIFY(CollectGenStepInFlight);
    PopGCBarriers(*CollectGenStepInFlight);
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("notice", "collect_gen_step")("value", *CollectGenStepInFlight)("current_gen", CurrentGen);

    const bool isFull = gcContext.IsFull();

    auto removeCategories = sharedBlobsInfo->BuildRemoveCategories(std::move(gcContext.MutableExtractedToRemoveFromDB()));

    auto result = std::make_shared<NBlobOperations::NBlobStorage::TGCTask>(storageId, std::move(gcContext.MutablePerGroupGCListsInFlight()), *CollectGenStepInFlight,
        std::move(gcContext.MutableKeepsToErase()), manager, std::move(removeCategories), counters, TabletInfo->TabletID, CurrentGen);
    if (result->IsEmpty()) {
        CollectGenStepInFlight = {};
        return nullptr;
    }

    if (isFull) {
        PreviousGCTime = TInstant::Zero();
    }

    return result;
}

TBlobBatch TBlobManager::StartBlobBatch() {
    ++CurrentStep;
    AFL_VERIFY(TabletInfo->Channels.size() > 2);
    const auto& channel = TabletInfo->Channels[(CurrentStep % (TabletInfo->Channels.size() - 2)) + 2];
    ++CountersUpdate.BatchesStarted;
    TAllocatedGenStepConstPtr genStepRef = new TAllocatedGenStep({ CurrentGen, CurrentStep });
    AllocatedGenSteps.push_back(genStepRef);
    auto batchInfo = std::make_unique<TBlobBatch::TBatchInfo>(TabletInfo, genStepRef, channel.Channel, BlobsManagerCounters);
    return TBlobBatch(std::move(batchInfo));
}

void TBlobManager::DoSaveBlobBatchOnComplete(TBlobBatch&& blobBatch) {
    Y_ABORT_UNLESS(blobBatch.BatchInfo);
    ++CountersUpdate.BatchesCommitted;
    CountersUpdate.BlobsWritten += blobBatch.GetBlobCount();

    LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID
        << " Save Batch GenStep: " << blobBatch.BatchInfo->Gen << ":" << blobBatch.BatchInfo->Step
        << " Blob count: " << blobBatch.BatchInfo->GetBlobIds().size());

    // Add this batch to KeepQueue
    TGenStep edgeGenStep = EdgeGenStep();
    for (auto&& blobId : blobBatch.BatchInfo->GetBlobIds()) {
        auto logoBlobId = blobId.GetLogoBlobId();
        TGenStep genStep{ logoBlobId.Generation(), logoBlobId.Step() };

        AFL_VERIFY(genStep > edgeGenStep)("gen_step", genStep)("edge_gen_step", edgeGenStep)("blob_id", blobId.ToStringNew());
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_keep", logoBlobId.ToString());

        BlobsManagerCounters.OnKeepMarker(logoBlobId.BlobSize());
        AFL_VERIFY(BlobsToKeep[genStep].emplace(logoBlobId).second);
    }
    BlobsManagerCounters.OnBlobsKeep(BlobsToKeep);

    blobBatch.BatchInfo->GenStepRef.Reset();
}

void TBlobManager::DoSaveBlobBatchOnExecute(const TBlobBatch& blobBatch, IBlobManagerDb& db) {
    Y_ABORT_UNLESS(blobBatch.BatchInfo);
    LOG_S_DEBUG("BlobManager on execute at tablet " << TabletInfo->TabletID
        << " Save Batch GenStep: " << blobBatch.BatchInfo->Gen << ":" << blobBatch.BatchInfo->Step
        << " Blob count: " << blobBatch.BatchInfo->GetBlobIds().size());

    TGenStep edgeGenStep = EdgeGenStep();
    for (auto&& blobId : blobBatch.BatchInfo->GetBlobIds()) {
        auto logoBlobId = blobId.GetLogoBlobId();
        TGenStep genStep{ logoBlobId.Generation(), logoBlobId.Step() };

        AFL_VERIFY(genStep > edgeGenStep)("gen_step", genStep)("edge_gen_step", edgeGenStep)("blob_id", blobId.ToStringNew());
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_keep_on_execute", logoBlobId.ToString());
        db.AddBlobToKeep(blobId);
    }
}

void TBlobManager::DeleteBlobOnExecute(const TTabletId tabletId, const TUnifiedBlobId& blobId, IBlobManagerDb& db) {
    // Persist deletion intent
    db.AddBlobToDelete(blobId, tabletId);
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_delete_on_execute", blobId);
}

void TBlobManager::DeleteBlobOnComplete(const TTabletId tabletId, const TUnifiedBlobId& blobId) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_delete_on_complete", blobId)("tablet_id_delete", (ui64)tabletId);
    ++CountersUpdate.BlobsDeleted;

    // Check if the deletion needs to be delayed until the blob is no longer
    // used by in-flight requests
    if (!IsBlobInUsage(blobId)) {
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delete Blob " << blobId);
        AFL_VERIFY(BlobsToDelete.Add(tabletId, blobId));
        BlobsManagerCounters.OnDeleteBlobMarker(blobId.BlobSize());
        BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
    } else {
        BlobsManagerCounters.OnDeleteBlobDelayedMarker(blobId.BlobSize());
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delay Delete Blob " << blobId);
        BlobsToDeleteDelayed.Add(tabletId, blobId);
    }
}

void TBlobManager::OnGCFinishedOnExecute(const TGenStep& genStep, IBlobManagerDb& db) {
    db.SaveLastGcBarrier(genStep);
}

void TBlobManager::OnGCFinishedOnComplete(const TGenStep& genStep) {
    LastCollectedGenStep = genStep;
    CollectGenStepInFlight.reset();
}

void TBlobManager::OnGCStartOnExecute(const TGenStep& genStep, IBlobManagerDb& db) {
    db.SaveGCBarrierPreparation(genStep);
}

void TBlobManager::OnGCStartOnComplete(const TGenStep& genStep) {
    GCBarrierPreparation = genStep;
}

void TBlobManager::OnBlobFree(const TUnifiedBlobId& blobId) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "blob_free")("blob_id", blobId);
    // Check if the blob is marked for delayed deletion
    if (BlobsToDeleteDelayed.ExtractBlobTo(blobId, BlobsToDelete)) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("blob_id", blobId)("event", "blob_delayed_deleted");
        BlobsManagerCounters.OnBlobsDelete(BlobsToDelete);
        BlobsManagerCounters.OnDeleteBlobMarker(blobId.GetLogoBlobId().BlobSize());
    }
}

}
