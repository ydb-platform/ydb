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
    BlobsManagerCounters.CurrentGen->Set(CurrentGen);
    BlobsManagerCounters.CurrentStep->Set(CurrentStep);
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
    AFL_VERIFY(!GCBarrierPreparation.Generation() || LastCollectedGenStep <= GCBarrierPreparation)("prepared", GCBarrierPreparation)("last", LastCollectedGenStep);

    // Load the keep and delete queues
    std::vector<TUnifiedBlobId> blobsToKeep;
    NColumnShard::TBlobGroupSelector dsGroupSelector(TabletInfo);
    if (!db.LoadLists(blobsToKeep, BlobsToDelete, &dsGroupSelector, selfTabletId)) {
        return false;
    }

    BlobsManagerCounters.OnBlobsToDelete(BlobsToDelete);

    // Build the list of steps that cannot be garbage collected before Keep flag is set on the blobs
    TBlobsByGenStep blobsToKeepLocal;
    for (const auto& unifiedBlobId : blobsToKeep) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("add_blob_to_keep", unifiedBlobId.ToStringNew());
        TLogoBlobID blobId = unifiedBlobId.GetLogoBlobId();
        Y_ABORT_UNLESS(LastCollectedGenStep < TGenStep(blobId));

        AFL_VERIFY(blobsToKeepLocal.Add(blobId))("blob_to_keep_double", unifiedBlobId.ToStringNew());
    }
    std::swap(blobsToKeepLocal, BlobsToKeep);
    BlobsManagerCounters.OnBlobsToKeep(BlobsToKeep);

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
    if (AllocatedGenSteps.empty() && LastCollectedGenStep < TGenStep(CurrentGen, CurrentStep)) {
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
    return result;
}

class TBlobManager::TGCContext {
private:
    static inline const ui32 BlobsGCCountLimit = 500000;
    YDB_ACCESSOR_DEF(NBlobOperations::NBlobStorage::TGCTask::TGCListsByGroup, PerGroupGCListsInFlight);
    YDB_ACCESSOR_DEF(TTabletsByBlob, ExtractedToRemoveFromDB);
    YDB_ACCESSOR_DEF(std::deque<TUnifiedBlobId>, KeepsToErase);
    YDB_READONLY_DEF(std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>, SharedBlobsManager);
public:
    ui64 GetKeepBytes() const {
        ui64 size = 0;
        for (auto&& i : KeepsToErase) {
            size += i.BlobSize();
        }
        return size;
    }

    ui64 GetDeleteBytes() const {
        ui64 size = 0;
        for (TTabletsByBlob::TIterator it(ExtractedToRemoveFromDB); it.IsValid(); ++it) {
            size += it.GetBlobId().BlobSize();
        }
        return size;
    }

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

    for (TTabletsByBlob::TIterator it(extractedOld); it.IsValid(); ++it) {
        const auto& unifiedBlobId = it.GetBlobId();
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

bool TBlobManager::DrainKeepTo(const TGenStep& dest, TGCContext& gcContext) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "PreparePerGroupGCRequests")("gen_step", dest)("gs_blobs_to_keep_count", BlobsToKeep.GetSize());

    const auto pred = [&](const TGenStep& genStep, const TLogoBlobID& logoBlobId) {
        AFL_VERIFY(LastCollectedGenStep < genStep)("last", LastCollectedGenStep.ToString())("gen", genStep.ToString());
        const ui32 blobGroup = TabletInfo->GroupFor(logoBlobId.Channel(), logoBlobId.Generation());
        TBlobAddress bAddress(blobGroup, logoBlobId.Channel());
        const TUnifiedBlobId keepUnified(blobGroup, logoBlobId);
        gcContext.MutableKeepsToErase().emplace_back(keepUnified);
        if (BlobsToDelete.ExtractBlobTo(keepUnified, gcContext.MutableExtractedToRemoveFromDB())) {
            if (logoBlobId.Generation() == CurrentGen) {
                AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_not_keep", keepUnified.ToStringNew());
                return;
            }
            if (gcContext.GetSharedBlobsManager()->BuildStoreCategories({ keepUnified }).GetDirect().IsEmpty()) {
                AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_not_keep_not_direct", keepUnified.ToStringNew());
                return;
            }
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_not_keep_old", keepUnified.ToStringNew());
            gcContext.MutablePerGroupGCListsInFlight()[bAddress].DontKeepList.insert(logoBlobId);
        } else {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("to_keep", keepUnified.ToStringNew());
            gcContext.MutablePerGroupGCListsInFlight()[bAddress].KeepList.insert(logoBlobId);
        }
    };

    return BlobsToKeep.ExtractTo(dest, gcContext.GetFreeSpace(), pred);
}

std::shared_ptr<NBlobOperations::NBlobStorage::TGCTask> TBlobManager::BuildGCTask(const TString& storageId,
    const std::shared_ptr<TBlobManager>& manager, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobsInfo,
    const std::shared_ptr<NBlobOperations::TRemoveGCCounters>& counters) noexcept {
    AFL_VERIFY(!CollectGenStepInFlight);
    if (BlobsToKeep.IsEmpty() && BlobsToDelete.IsEmpty() && LastCollectedGenStep == TGenStep{ CurrentGen, CurrentStep }) {
        BlobsManagerCounters.GCCounters.SkipCollectionEmpty->Add(1);
        ACFL_DEBUG("event", "TBlobManager::BuildGCTask skip")("current_gen", CurrentGen)("current_step", CurrentStep)("reason", "empty");
        return nullptr;
    }

    if (AppData()->TimeProvider->Now() - PreviousGCTime < NYDBTest::TControllers::GetColumnShardController()->GetOverridenGCPeriod()) {
        ACFL_DEBUG("event", "TBlobManager::BuildGCTask skip")("current_gen", CurrentGen)("current_step", CurrentStep)("reason", "too_often");
        BlobsManagerCounters.GCCounters.SkipCollectionThrottling->Add(1);
        return nullptr;
    }

    PreviousGCTime = AppData()->TimeProvider->Now();
    TGCContext gcContext(sharedBlobsInfo);
    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("action_id", TGUID::CreateTimebased().AsGuidString());
    const std::deque<TGenStep> newCollectGenSteps = FindNewGCBarriers();
    if (GCBarrierPreparation != LastCollectedGenStep) {
        if (GCBarrierPreparation.Generation()) {
            AFL_VERIFY(GCBarrierPreparation.Generation() < CurrentGen);
            AFL_VERIFY(LastCollectedGenStep <= GCBarrierPreparation);
            if (DrainKeepTo(GCBarrierPreparation, gcContext)) {
                CollectGenStepInFlight = GCBarrierPreparation;
            }
        }
    } else {
        DrainDeleteTo(LastCollectedGenStep, gcContext);
    }
    for (auto&& newCollectGenStep : newCollectGenSteps) {
        if (!DrainKeepTo(newCollectGenStep, gcContext)) {
            break;
        }
        if (newCollectGenStep.Generation() == CurrentGen) {
            CollectGenStepInFlight = std::max(CollectGenStepInFlight.value_or(newCollectGenStep), newCollectGenStep);
        }
    }

    if (CollectGenStepInFlight) {
        PopGCBarriers(*CollectGenStepInFlight);
        if (FirstGC) {
            gcContext.InitializeFirst(TabletInfo);
            FirstGC = false;
        }
        if (!BlobsToKeep.IsEmpty()) {
            AFL_VERIFY(*CollectGenStepInFlight < BlobsToKeep.GetMinGenStepVerified())("gs", *CollectGenStepInFlight)("first", BlobsToKeep.GetMinGenStepVerified());
        }
        AFL_VERIFY(LastCollectedGenStep < *CollectGenStepInFlight);
    }
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("notice", "collect_gen_step")("value", CollectGenStepInFlight)("current_gen", CurrentGen);

    if (gcContext.IsFull()) {
        PreviousGCTime = TInstant::Zero();
    }

    BlobsManagerCounters.GCCounters.OnGCTask(gcContext.GetKeepsToErase().size(), gcContext.GetKeepBytes(),
        gcContext.GetExtractedToRemoveFromDB().GetSize(), gcContext.GetDeleteBytes(), gcContext.IsFull(), !!CollectGenStepInFlight);
    auto removeCategories = sharedBlobsInfo->BuildRemoveCategories(std::move(gcContext.MutableExtractedToRemoveFromDB()));
    auto result = std::make_shared<NBlobOperations::NBlobStorage::TGCTask>(storageId, std::move(gcContext.MutablePerGroupGCListsInFlight()), 
        CollectGenStepInFlight, std::move(gcContext.MutableKeepsToErase()), manager, std::move(removeCategories), counters, TabletInfo->TabletID, CurrentGen);
    if (result->IsEmpty()) {
        BlobsManagerCounters.GCCounters.OnEmptyGCTask();
        CollectGenStepInFlight = {};
        return nullptr;
    }

    return result;
}

TBlobBatch TBlobManager::StartBlobBatch() {
    AFL_VERIFY(++CurrentStep < Max<ui32>() - 10);
    BlobsManagerCounters.CurrentStep->Set(CurrentStep);
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

        AFL_VERIFY(BlobsToKeep.Add(logoBlobId));
        BlobsManagerCounters.OnBlobsToKeep(BlobsToKeep);
    }
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
        BlobsManagerCounters.OnBlobsToDelete(BlobsToDelete);
    } else {
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delay Delete Blob " << blobId);
        AFL_VERIFY(BlobsToDeleteDelayed.Add(tabletId, blobId));
        BlobsManagerCounters.OnBlobsToDeleteDelayed(BlobsToDeleteDelayed);
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
        AFL_VERIFY(GCBarrierPreparation == LastCollectedGenStep)("prepare", GCBarrierPreparation)("last", LastCollectedGenStep);
        CollectGenStepInFlight.reset();
    } else {
        AFL_VERIFY(!CollectGenStepInFlight);
    }
}

void TBlobManager::OnGCStartOnExecute(const std::optional<TGenStep>& genStep, IBlobManagerDb& db) {
    if (genStep) {
        AFL_VERIFY(LastCollectedGenStep < *genStep)("last", LastCollectedGenStep)("prepared", genStep);
        db.SaveGCBarrierPreparation(*genStep);
    }
}

void TBlobManager::OnGCStartOnComplete(const std::optional<TGenStep>& genStep) {
    if (genStep) {
        AFL_VERIFY(GCBarrierPreparation <= *genStep)("last", GCBarrierPreparation)("prepared", genStep);
        GCBarrierPreparation = *genStep;
    }
}

void TBlobManager::OnBlobFree(const TUnifiedBlobId& blobId) {
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "blob_free")("blob_id", blobId);
    // Check if the blob is marked for delayed deletion
    if (BlobsToDeleteDelayed.ExtractBlobTo(blobId, BlobsToDelete)) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("blob_id", blobId)("event", "blob_delayed_deleted");
        BlobsManagerCounters.OnBlobsToDelete(BlobsToDelete);
        BlobsManagerCounters.OnBlobsToDeleteDelayed(BlobsToDeleteDelayed);
    }
}

}
