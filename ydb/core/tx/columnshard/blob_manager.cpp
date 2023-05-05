#include "defs.h"
#include "columnshard_impl.h"
#include "blob_manager.h"
#include "blob_manager_db.h"
#include "blob_cache.h"

#include <ydb/core/base/blobstorage.h>

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
    const ui32 Gen;
    const ui32 Step;
    const ui32 Channel;

    TVector<ui32> BlobSizes;
    TVector<bool> InFlight;
    i32 InFlightCount;
    ui64 TotalSizeBytes;
    TVector<TString> SmallBlobs;

    TBatchInfo(TIntrusivePtr<TTabletStorageInfo> tabletInfo, TAllocatedGenStepConstPtr genStep, ui32 channel)
        : TabletInfo(tabletInfo)
        , GenStepRef(genStep)
        , Gen(std::get<0>(GenStepRef->GenStep))
        , Step(std::get<1>(GenStepRef->GenStep))
        , Channel(channel)
        , InFlightCount(0)
        , TotalSizeBytes(0)
    {}

    TUnifiedBlobId NextBlobId(ui32 blobSize) {
        BlobSizes.push_back(blobSize);
        InFlight.push_back(true);
        ++InFlightCount;
        TotalSizeBytes += blobSize;
        return MakeBlobId(BlobSizes.size()-1);
    }

    TUnifiedBlobId MakeBlobId(ui32 i) const {
        Y_VERIFY(i < BlobSizes.size());
        const ui32 dsGroup = TabletInfo->GroupFor(Channel, Gen);
        return TUnifiedBlobId(dsGroup, TLogoBlobID(TabletInfo->TabletID, Gen, Step, Channel, BlobSizes[i], i));
    }

    TUnifiedBlobId AddSmallBlob(const TString& data) {
        // NOTE: small blobs are not included into TotalSizeBytes
        SmallBlobs.push_back(data);
        return MakeSmallBlobId(SmallBlobs.size()-1);
    }

    TUnifiedBlobId MakeSmallBlobId(ui32 i) const {
        Y_VERIFY(i < SmallBlobs.size());
        return TUnifiedBlobId(TabletInfo->TabletID, Gen, Step, i, SmallBlobs[i].size());
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

TUnifiedBlobId TBlobBatch::SendWriteBlobRequest(const TString& blobData, TInstant deadline, const TActorContext& ctx) {
    Y_VERIFY(blobData.size() <= TLimits::MAX_BLOB_SIZE, "Blob %" PRISZT" size exceeds the limit %" PRIu64,
        blobData.size(), TLimits::MAX_BLOB_SIZE);

    TUnifiedBlobId blobId = BatchInfo->NextBlobId(blobData.size());
    ui32 groupId = blobId.GetDsGroup();

    SendWriteRequest(ctx, groupId, blobId.GetLogoBlobId(), blobData, 0, deadline);

    return blobId;
}

void TBlobBatch::OnBlobWriteResult(TEvBlobStorage::TEvPutResult::TPtr& ev) {
    TLogoBlobID blobId = ev->Get()->Id;
    Y_VERIFY(ev->Get()->Status == NKikimrProto::OK, "The caller must handle unsuccessful status");
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


TUnifiedBlobId TBlobBatch::AddSmallBlob(const TString& data) {
    Y_VERIFY(BatchInfo);
    return BatchInfo->AddSmallBlob(data);
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
    TVector<TUnifiedBlobId> blobsToKeep;
    TVector<TUnifiedBlobId> blobsToDelete;
    TBlobGroupSelector dsGroupSelector(TabletInfo);
    if (!db.LoadLists(blobsToKeep, blobsToDelete, &dsGroupSelector)) {
        return false;
    }

    for (const auto& unifiedBlobId : blobsToDelete) {
        if (unifiedBlobId.IsSmallBlob()) {
            SmallBlobsToDelete.insert(unifiedBlobId);
        } else if (unifiedBlobId.IsDsBlob()) {
            BlobsToDelete.insert(unifiedBlobId.GetLogoBlobId());
        } else {
            Y_FAIL("Unexpected blob id: %s", unifiedBlobId.ToStringNew().c_str());
        }
    }

    // Build the list of steps that cannot be garbage collected before Keep flag is set on the blobs
    THashSet<TGenStep> genStepsWithBlobsToKeep;
    for (const auto& unifiedBlobId : blobsToKeep) {
        Y_VERIFY(unifiedBlobId.IsDsBlob(), "Not a DS blob id in Keep table: %s", unifiedBlobId.ToStringNew().c_str());

        TLogoBlobID blobId = unifiedBlobId.GetLogoBlobId();
        TGenStep genStep{blobId.Generation(), blobId.Step()};
        if (genStep <= LastCollectedGenStep) {
            LOG_S_WARN("BlobManager at tablet " << TabletInfo->TabletID
                << " Load not keeped blob " << unifiedBlobId.ToStringNew() << " collected by GenStep: "
                << std::get<0>(LastCollectedGenStep) << ":" << std::get<1>(LastCollectedGenStep));
            KeepsToErase.emplace_back(unifiedBlobId);
            continue;
        }

        BlobsToKeep.insert(blobId);

        // Keep + DontKeep (probably in different gen:steps)
        // GC could go through it to a greater LastCollectedGenStep
        if (BlobsToDelete.count(blobId)) {
            continue;
        }

        genStepsWithBlobsToKeep.insert(genStep);
    }

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

bool TBlobManager::CanCollectGarbage(bool cleanupOnly) const {
    if (KeepsToErase.size() || DeletesToErase.size()) {
        return true;
    }
    if (cleanupOnly) {
        return false;
    }
    return NeedStorageCG();
}

bool TBlobManager::NeedStorageCG() const {
    // Check that there is no GC request in flight
    if (!PerGroupGCListsInFlight.empty()) {
        return false;
    }

    if (BlobsToKeep.empty() && BlobsToDelete.empty() && LastCollectedGenStep == TGenStep{CurrentGen, CurrentStep}) {
        return false;
    }

    // Delay GC if there are to few blobs and last GC was not long ago
    if ((i64)BlobsToKeep.size() < BlobCountToTriggerGC &&
        (i64)BlobsToDelete.size() < BlobCountToTriggerGC &&
        PreviousGCTime + TDuration::Seconds(GCIntervalSeconds) > AppData()->TimeProvider->Now())
    {
        return false;
    }

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

THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> TBlobManager::PreparePerGroupGCRequests() {
    if (!NeedStorageCG()) {
        return {};
    }

    TGenStep newCollectGenStep = FindNewGCBarrier();
    Y_VERIFY(newCollectGenStep >= LastCollectedGenStep);
    if (newCollectGenStep == LastCollectedGenStep) {
        return {};
    }

    PreviousGCTime = AppData()->TimeProvider->Now();

    const ui32 channelIdx = BLOB_CHANNEL;

    Y_VERIFY(PerGroupGCListsInFlight.empty());

    // Clear all possibly not keeped trash in channel's groups: create an event for each group
    if (FirstGC) {
        FirstGC = false;

        // TODO: we need only actual channel history here
        const auto& channelHistory = TabletInfo->ChannelInfo(channelIdx)->History;

        for (auto it = channelHistory.begin(); it != channelHistory.end(); ++it) {
            PerGroupGCListsInFlight[it->GroupID];
        }
    }

    // Make per-group Keep/DontKeep lists
    {
        // Add all blobs to keep
        auto keepBlobIt = BlobsToKeep.begin();
        for (; keepBlobIt != BlobsToKeep.end(); ++keepBlobIt) {
            TGenStep genStep{keepBlobIt->Generation(), keepBlobIt->Step()};
            if (genStep > newCollectGenStep) {
                break;
            }
            ui32 blobGroup = TabletInfo->GroupFor(keepBlobIt->Channel(), keepBlobIt->Generation());
            PerGroupGCListsInFlight[blobGroup].KeepList.insert(*keepBlobIt);
        }
        if (BlobsToKeep.begin() != keepBlobIt) {
            BlobsToKeep.erase(BlobsToKeep.begin(), keepBlobIt);
        }

        // Add all blobs to delete
        auto blobIt = BlobsToDelete.begin();
        for (; blobIt != BlobsToDelete.end(); ++blobIt) {
            TGenStep genStep{blobIt->Generation(), blobIt->Step()};
            if (genStep > newCollectGenStep) {
                break;
            }
            ui32 blobGroup = TabletInfo->GroupFor(blobIt->Channel(), blobIt->Generation());
            TGCLists& gl = PerGroupGCListsInFlight[blobGroup];
            bool skipDontKeep = false;
            if (gl.KeepList.count(*blobIt)) {
                // Remove the blob from keep list if its also in the delete list
                gl.KeepList.erase(*blobIt);
                // Skipped blobs still need to be deleted from BlobsToKeep table
                KeepsToErase.emplace_back(TUnifiedBlobId(blobGroup, *blobIt));

                if (CurrentGen == blobIt->Generation()) {
                    // If this blob was created and deleted in the current generation then
                    // we can skip sending both Keep and DontKeep flags.
                    // NOTE: its not safe to do this for older generations because there is
                    // a scenario when Keep flag was sent in the old generation and then tablet restarted
                    // before getting the result and removing the blob from the Keep list.
                    skipDontKeep = true;
                    DeletesToErase.emplace_back(TUnifiedBlobId(blobGroup, *blobIt));
                    ++CountersUpdate.BlobSkippedEntries;
                }
            }
            if (!skipDontKeep) {
                gl.DontKeepList.insert(*blobIt);
            }
        }
        if (BlobsToDelete.begin() != blobIt) {
            BlobsToDelete.erase(BlobsToDelete.begin(), blobIt);
        }
    }

    CollectGenStepInFlight = newCollectGenStep;

    // Make per group requests
    THashMap<ui32, std::unique_ptr<TEvBlobStorage::TEvCollectGarbage>> requests;
    {
        for (const auto& gl : PerGroupGCListsInFlight) {
            ui32 group = gl.first;
            requests[group] = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
                TabletInfo->TabletID, CurrentGen, PerGenerationCounter,
                channelIdx, true,
                std::get<0>(CollectGenStepInFlight), std::get<1>(CollectGenStepInFlight),
                new TVector<TLogoBlobID>(gl.second.KeepList.begin(), gl.second.KeepList.end()),
                new TVector<TLogoBlobID>(gl.second.DontKeepList.begin(), gl.second.DontKeepList.end()),
                TInstant::Max(), true);

            CounterToGroupInFlight[PerGenerationCounter] = group;

            PerGenerationCounter += requests[group]->PerGenerationCounterStepSize();
        }
    }

    return requests;
}

size_t TBlobManager::CleanupFlaggedBlobs(IBlobManagerDb& db, size_t maxBlobsToCleanup) {
    if (KeepsToErase.empty() && DeletesToErase.empty()) {
        return 0;
    }

    size_t numBlobs = 0;

    for (; !KeepsToErase.empty() && numBlobs < maxBlobsToCleanup; ++numBlobs) {
        db.EraseBlobToKeep(KeepsToErase.front());
        KeepsToErase.pop_front();
    }

    for (; !DeletesToErase.empty() && numBlobs < maxBlobsToCleanup; ++numBlobs) {
        db.EraseBlobToDelete(DeletesToErase.front());
        DeletesToErase.pop_front();
    }

    Y_VERIFY(numBlobs <= maxBlobsToCleanup);
    return numBlobs;
}

void TBlobManager::OnGCResult(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev, IBlobManagerDb& db) {
    Y_VERIFY(ev->Get()->Status == NKikimrProto::OK, "The caller must handle unsuccessful status");
    Y_VERIFY(!CounterToGroupInFlight.empty());
    Y_VERIFY(!PerGroupGCListsInFlight.empty());

    // Find the group for this result
    ui64 counterFromRequest = ev->Get()->PerGenerationCounter;
    Y_VERIFY(CounterToGroupInFlight.count(counterFromRequest));
    ui32 group = CounterToGroupInFlight[counterFromRequest];

    auto it = PerGroupGCListsInFlight.find(group);
    const auto& keepList = it->second.KeepList;
    const auto& dontKeepList = it->second.DontKeepList;

    // NOTE: It clears blobs of different groups.
    // It's expected to be safe cause we have GC result for the blobs or don't need such result.
    size_t maxBlobsToCleanup = TLimits::MAX_BLOBS_TO_DELETE;
    maxBlobsToCleanup -= CleanupFlaggedBlobs(db, maxBlobsToCleanup);

    size_t blobsToForget = keepList.size() + dontKeepList.size();

    if (blobsToForget < maxBlobsToCleanup) {
        for (const auto& blobId : keepList) {
            db.EraseBlobToKeep(TUnifiedBlobId(group, blobId));
        }
        for (const auto& blobId : dontKeepList) {
            db.EraseBlobToDelete(TUnifiedBlobId(group, blobId));
        }
    } else {
        for (const auto& blobId : keepList) {
            KeepsToErase.emplace_back(TUnifiedBlobId(group, blobId));
        }
        for (const auto& blobId : dontKeepList) {
            DeletesToErase.emplace_back(TUnifiedBlobId(group, blobId));
        }
    }

    ++CountersUpdate.GcRequestsSent;
    CountersUpdate.BlobKeepEntries += keepList.size();
    CountersUpdate.BlobDontKeepEntries += dontKeepList.size();

    PerGroupGCListsInFlight.erase(it);
    CounterToGroupInFlight.erase(group);

    // All requests done?
    if (PerGroupGCListsInFlight.empty()) {
        LastCollectedGenStep = CollectGenStepInFlight;
        db.SaveLastGcBarrier(LastCollectedGenStep);
    }

    PerformDelayedDeletes(db);
}

TBlobBatch TBlobManager::StartBlobBatch(ui32 channel) {
    ++CountersUpdate.BatchesStarted;
    Y_VERIFY(channel == BLOB_CHANNEL, "Support for mutiple blob channels is not implemented yet");
    ++CurrentStep;
    TAllocatedGenStepConstPtr genStepRef = new TAllocatedGenStep({CurrentGen, CurrentStep});
    AllocatedGenSteps.push_back(genStepRef);
    auto batchInfo = std::make_unique<TBlobBatch::TBatchInfo>(TabletInfo, genStepRef, channel);
    return TBlobBatch(std::move(batchInfo));
}

void TBlobManager::SaveBlobBatch(TBlobBatch&& blobBatch, IBlobManagerDb& db) {
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

        Y_VERIFY(genStep > edgeGenStep,
            "Trying to keep blob %s that could be already collected by edge barrier (%" PRIu32 ":%" PRIu32 ")",
            blobId.ToStringNew().c_str(), std::get<0>(edgeGenStep), std::get<1>(edgeGenStep));

        BlobsToKeep.insert(std::move(logoblobId));
        db.AddBlobToKeep(blobId);
    }

    // Save all small blobs
    for (ui32 i = 0; i < blobBatch.BatchInfo->SmallBlobs.size(); ++i) {
        const TUnifiedBlobId blobId = blobBatch.BatchInfo->MakeSmallBlobId(i);
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Save Small Blob " << blobId);
        db.WriteSmallBlob(blobId, blobBatch.BatchInfo->SmallBlobs[i]);
        ++CountersUpdate.SmallBlobsWritten;
        CountersUpdate.SmallBlobsBytesWritten += blobId.BlobSize();
    }

    blobBatch.BatchInfo->GenStepRef.Reset();
}

void TBlobManager::DeleteBlob(const TUnifiedBlobId& blobId, IBlobManagerDb& db) {
    PerformDelayedDeletes(db);

    ++CountersUpdate.BlobsDeleted;

    if (blobId.IsSmallBlob()) {
        if (BlobsUseCount.count(blobId) == 0) {
            DeleteSmallBlob(blobId, db);
        } else {
            LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delay Delete Small Blob " << blobId);
            db.AddBlobToDelete(blobId);
            SmallBlobsToDeleteDelayed.insert(blobId);
        }
        return;
    }

    // Persist deletion intent
    db.AddBlobToDelete(blobId);

    // Check if the deletion needs to be delayed until the blob is no longer
    // used by in-flight requests
    if (BlobsUseCount.count(blobId) == 0) {
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delete Blob " << blobId);
        TLogoBlobID logoBlobId = blobId.GetLogoBlobId();
        BlobsToDelete.insert(logoBlobId);
        NBlobCache::ForgetBlob(blobId);
    } else {
        LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delay Delete Blob " << blobId);
        BlobsToDeleteDelayed.insert(blobId.GetLogoBlobId());
    }
}

bool TBlobManager::ExportOneToOne(TEvictedBlob&& evict, const NKikimrTxColumnShard::TEvictMetadata& meta,
                                  IBlobManagerDb& db)
{
    if (EvictedBlobs.count(evict) || DroppedEvictedBlobs.count(evict)) {
        return false;
    }

    TString strMeta;
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&strMeta);

    db.UpdateEvictBlob(evict, strMeta);
    EvictedBlobs.emplace(std::move(evict), meta);
    return true;
}

bool TBlobManager::DropOneToOne(const TUnifiedBlobId& blobId, IBlobManagerDb& db) {
    NOlap::TEvictedBlob evict{
        .State = EEvictState::UNKNOWN,
        .Blob = blobId
    };

    TEvictMetadata meta;
    bool extracted = ExtractEvicted(evict, meta);
    if (!extracted) {
        return false; // It's not at exported blob.
    }
#if 0 // TODO: SELF_CACHED logic
    if (evict.State == EEvictState::SELF_CACHED) {
        evict.State = EEvictState::EXTERN; // SELF_CACHED -> EXTERN for dropped
    }
#endif
    db.DropEvictBlob(evict);
    DroppedEvictedBlobs.emplace(std::move(evict), std::move(meta));
    return true;
}

bool TBlobManager::UpdateOneToOne(TEvictedBlob&& evict, IBlobManagerDb& db, bool& dropped) {
    TEvictMetadata meta;

    TEvictedBlob old{.Blob = evict.Blob};
    bool extracted = ExtractEvicted(old, meta);
    dropped = false;
    if (!extracted) {
        dropped = DroppedEvictedBlobs.count(evict);
        if (!dropped) {
            return false; // update after erase
        }
        extracted = ExtractEvicted(old, meta, true);
    }
    Y_VERIFY(extracted);

    switch (evict.State) {
        case EEvictState::EVICTING:
            Y_FAIL();
        case EEvictState::SELF_CACHED:
            Y_VERIFY(old.State == EEvictState::EVICTING);
            break;
        case EEvictState::EXTERN:
            Y_VERIFY(old.State == EEvictState::EVICTING || old.State == EEvictState::SELF_CACHED);
            break;
        default:
            break;
    }

    if (dropped) {
        if (evict.State == EEvictState::SELF_CACHED) {
            evict.State = EEvictState::EXTERN; // SELF_CACHED -> EXTERN for dropped
        }
        DroppedEvictedBlobs.emplace(evict, meta);
    } else {
        EvictedBlobs.emplace(evict, meta);
    }

    // TODO: update meta if needed
    db.UpdateEvictBlob(evict, {});
    return true;
}

bool TBlobManager::EraseOneToOne(const TEvictedBlob& evict, IBlobManagerDb& db) {
    db.EraseEvictBlob(evict);
    return DroppedEvictedBlobs.erase(evict);
}

bool TBlobManager::LoadOneToOneExport(IBlobManagerDb& db, THashSet<TUnifiedBlobId>& droppedEvicting) {
    EvictedBlobs.clear();
    DroppedEvictedBlobs.clear();

    TBlobGroupSelector dsGroupSelector(TabletInfo);
    THashMap<TEvictedBlob, TString> evicted;
    THashMap<TEvictedBlob, TString> dropped;
    if (!db.LoadEvicted(evicted, dropped, dsGroupSelector)) {
        return false;
    }

    for (auto& [evict, metadata] : evicted) {
        NKikimrTxColumnShard::TEvictMetadata meta;
        Y_VERIFY(meta.ParseFromString(metadata));

        EvictedBlobs.emplace(evict, meta);
    }

    for (auto& [evict, metadata] : dropped) {
        if (evict.IsEvicting()) {
            droppedEvicting.insert(evict.Blob);
        }

        NKikimrTxColumnShard::TEvictMetadata meta;
        Y_VERIFY(meta.ParseFromString(metadata));

        DroppedEvictedBlobs.emplace(evict, meta);
    }

    return true;
}

TEvictedBlob TBlobManager::GetEvicted(const TUnifiedBlobId& blobId, TEvictMetadata& meta) {
    auto it = EvictedBlobs.find(TEvictedBlob{.Blob = blobId});
    if (it != EvictedBlobs.end()) {
        meta = it->second;
        return it->first;
    }
    return {};
}

TEvictedBlob TBlobManager::GetDropped(const TUnifiedBlobId& blobId, TEvictMetadata& meta) {
    auto it = DroppedEvictedBlobs.find(TEvictedBlob{.Blob = blobId});
    if (it != DroppedEvictedBlobs.end()) {
        meta = it->second;
        return it->first;
    }
    return {};
}

void TBlobManager::GetCleanupBlobs(THashSet<TEvictedBlob>& cleanup) const {
    TString strBlobs;
    for (auto& [evict, _] : DroppedEvictedBlobs) {
        if (evict.State != EEvictState::EVICTING) {
            strBlobs += "'" + evict.Blob.ToStringNew() + "' ";
            cleanup.insert(evict);
        }
    }
    if (!strBlobs.empty()) {
        LOG_S_NOTICE("Cleanup evicted blobs " << strBlobs << "at tablet " << TabletInfo->TabletID);
    }
}

void TBlobManager::DeleteSmallBlob(const TUnifiedBlobId& blobId, IBlobManagerDb& db) {
    LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delete Small Blob " << blobId);
    db.EraseSmallBlob(blobId);
    NBlobCache::ForgetBlob(blobId);
    ++CountersUpdate.SmallBlobsDeleted;
    CountersUpdate.SmallBlobsBytesDeleted += blobId.BlobSize();
}

void TBlobManager::PerformDelayedDeletes(IBlobManagerDb& db) {
    for (const auto& blobId : SmallBlobsToDelete) {
        DeleteSmallBlob(blobId, db);
        db.EraseBlobToDelete(blobId);
    }
    SmallBlobsToDelete.clear();
}

bool TBlobManager::BlobInUse(const NOlap::TUnifiedBlobId& blobId) const {
    return BlobsUseCount.count(blobId);
}

void TBlobManager::SetBlobInUse(const TUnifiedBlobId& blobId, bool inUse) {
    if (inUse) {
        BlobsUseCount[blobId]++;
        return;
    }

    auto useIt = BlobsUseCount.find(blobId);
    Y_VERIFY(useIt != BlobsUseCount.end(), "Trying to un-use an unknown blob %s",  blobId.ToStringNew().c_str());
    --useIt->second;

    if (useIt->second > 0) {
        // Blob is still in use
        return;
    }

    BlobsUseCount.erase(useIt);

    // Check if the blob is marked for delayed deletion
    if (blobId.IsSmallBlob()) {
        if (SmallBlobsToDeleteDelayed.erase(blobId)) {
            LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delayed Small Blob " << blobId
                << " is no longer in use" );
            SmallBlobsToDelete.insert(blobId);
        }
    } else {
        TLogoBlobID logoBlobId = blobId.GetLogoBlobId();
        if (BlobsToDeleteDelayed.erase(logoBlobId)) {
            LOG_S_DEBUG("BlobManager at tablet " << TabletInfo->TabletID << " Delete Delayed Blob " << blobId);
            BlobsToDelete.insert(logoBlobId);
            NBlobCache::ForgetBlob(blobId);
        }
    }
}

bool TBlobManager::ExtractEvicted(TEvictedBlob& evict, TEvictMetadata& meta, bool fromDropped /*= false*/) {
    if (fromDropped) {
        if (DroppedEvictedBlobs.count(evict)) {
            auto node = DroppedEvictedBlobs.extract(evict);
            if (!node.empty()) {
                evict = node.key();
                meta = node.mapped();
                return true;
            }
        }
    } else {
        if (EvictedBlobs.count(evict)) {
            auto node = EvictedBlobs.extract(evict);
            if (!node.empty()) {
                evict = node.key();
                meta = node.mapped();
                return true;
            }
        }
    }
    return false;
}

}
