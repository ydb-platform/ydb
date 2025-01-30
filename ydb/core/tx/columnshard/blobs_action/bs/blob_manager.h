#pragma once

#include "address.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/counters/blobs_manager.h>

#include <ydb/core/tablet_flat/flat_executor.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

#include <util/generic/string.h>
#include <map>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {
class TGCTask;
}

namespace NKikimr::NOlap {

using NKikimrTxColumnShard::TEvictMetadata;


// A batch of blobs that are written by a single task.
// The batch is later saved or discarded as a whole.
class TBlobBatch : public TMoveOnly {
    friend class TBlobManager;

    struct TBatchInfo;

    std::unique_ptr<TBatchInfo> BatchInfo;

private:
    explicit TBlobBatch(std::unique_ptr<TBatchInfo> batchInfo);

    void SendWriteRequest(const TActorContext& ctx, ui32 groupId, const TLogoBlobID& logoBlobId,
        const TString& data, ui64 cookie, TInstant deadline);

public:
    TBlobBatch();
    TBlobBatch(TBlobBatch&& other);
    TBlobBatch& operator = (TBlobBatch&& other);
    ~TBlobBatch();

    bool operator!() const {
        return !BatchInfo;
    }

    // Write new blob as a part of this batch
    void SendWriteBlobRequest(const TString& blobData, const TUnifiedBlobId& blobId, TInstant deadline, const TActorContext& ctx);

    TUnifiedBlobId AllocateNextBlobId(const TString& blobData);

    // Called with the result of WriteBlob request
    void OnBlobWriteResult(const TLogoBlobID& blobId, const NKikimrProto::EReplyStatus status);

    // Tells if all WriteBlob requests got corresponding results
    bool AllBlobWritesCompleted() const;

    // Number of blobs in the batch
    ui64 GetBlobCount() const;

    // Size of all blobs in the batch
    ui64 GetTotalSize() const;
};

class IBlobManagerDb;

// An interface for writing and deleting blobs for the ColumnShard index management.
// All garbage collection related logic is hidden inside the implementation.
class IBlobManager {
protected:
    virtual void DoSaveBlobBatchOnExecute(const TBlobBatch& blobBatch, IBlobManagerDb& db) = 0;
    virtual void DoSaveBlobBatchOnComplete(TBlobBatch&& blobBatch) = 0;
public:
    virtual ~IBlobManager() = default;

    // Allocates a temporary blob batch with the BlobManager. If the tablet crashes or if
    // this object is destroyed without doing SaveBlobBatch then all blobs in this batch
    // will get garbage-collected.
    virtual TBlobBatch StartBlobBatch() = 0;

    // This method is called in the same transaction in which the user saves references to blobs
    // in some LocalDB table. It tells the BlobManager that the blobs are becoming permanently saved.
    // NOTE: At this point all blob writes must be already acknowledged.
    void SaveBlobBatchOnExecute(const TBlobBatch& blobBatch, IBlobManagerDb& db) {
        if (blobBatch.GetBlobCount() == 0) {
            return;
        }
        return DoSaveBlobBatchOnExecute(blobBatch, db);
    }
    void SaveBlobBatchOnComplete(TBlobBatch&& blobBatch) {
        if (blobBatch.GetBlobCount() == 0) {
            return;
        }
        return DoSaveBlobBatchOnComplete(std::move(blobBatch));
    }

    virtual void DeleteBlobOnExecute(const TTabletId tabletId, const TUnifiedBlobId& blobId, IBlobManagerDb& db) = 0;
    virtual void DeleteBlobOnComplete(const TTabletId tabletId, const TUnifiedBlobId& blobId) = 0;
};

// A ref-counted object to keep track when GC barrier can be moved to some step.
// This means that all needed blobs below this step have been KeepFlag-ed and Ack-ed
struct TAllocatedGenStep : public TThrRefBase {
    const TGenStep GenStep;

    explicit TAllocatedGenStep(const TGenStep& genStep)
        : GenStep(genStep)
    {}

    bool Finished() const {
        return RefCount() == 1;
    }
};

using TAllocatedGenStepConstPtr = TIntrusiveConstPtr<TAllocatedGenStep>;

struct TBlobManagerCounters {
    ui64 BatchesStarted = 0;
    ui64 BatchesCommitted = 0;
    // TODO: ui64 BatchesDiscarded = 0; // Can we count them?
    ui64 BlobsWritten = 0;
    ui64 BlobsDeleted = 0;
    ui64 BlobKeepEntries = 0;
    ui64 BlobDontKeepEntries = 0;
    ui64 BlobSkippedEntries = 0;
    ui64 GcRequestsSent = 0;
};

// The implementation of BlobManager that hides all GC-related details
class TBlobManager : public IBlobManager, public TCommonBlobsTracker {
private:
    using TBlobAddress = NBlobOperations::NBlobStorage::TBlobAddress;
    class TGCContext;
    const TTabletId SelfTabletId;
    TIntrusivePtr<TTabletStorageInfo> TabletInfo;
    const ui32 CurrentGen;
    ui32 CurrentStep;
    std::optional<TGenStep> CollectGenStepInFlight;
    // Lists of blobs that need Keep flag to be set
    TBlobsByGenStep BlobsToKeep;
    // Lists of blobs that need DoNotKeep flag to be set
    TTabletsByBlob BlobsToDelete;

    // List of blobs that are marked for deletion but are still used by in-flight requests
    TTabletsByBlob BlobsToDeleteDelayed;

    // Sorted queue of GenSteps that have in-flight BlobBatches
    TDeque<TAllocatedGenStepConstPtr> AllocatedGenSteps;

    // The Gen:Step that has been acknowledged by the Distributed Storage
    TGenStep LastCollectedGenStep;
    TGenStep GCBarrierPreparation;

    // The barrier in the current in-flight GC request(s)
    bool FirstGC = true;

    const NColumnShard::TBlobsManagerCounters BlobsManagerCounters = NColumnShard::TBlobsManagerCounters("BlobsManager");

    // Stores counter updates since last call to GetCountersUpdate()
    // Then the counters are reset and start accumulating new delta
    TBlobManagerCounters CountersUpdate;

    TInstant PreviousGCTime; // Used for delaying next GC if there are too few blobs to collect

    virtual void DoSaveBlobBatchOnExecute(const TBlobBatch& blobBatch, IBlobManagerDb& db) override;
    virtual void DoSaveBlobBatchOnComplete(TBlobBatch&& blobBatch) override;
    void DrainDeleteTo(const TGenStep& dest, TGCContext& gcContext);
    [[nodiscard]] bool DrainKeepTo(const TGenStep& dest, TGCContext& gcContext);
public:
    TBlobManager(TIntrusivePtr<TTabletStorageInfo> tabletInfo, const ui32 gen, const TTabletId selfTabletId);

    bool HasToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) const {
        return BlobsToDelete.Contains(tabletId, blobId) || BlobsToDeleteDelayed.Contains(tabletId, blobId);
    }

    TTabletsByBlob GetBlobsToDeleteAll() const {
        auto result = BlobsToDelete;
        result.Add(BlobsToDeleteDelayed);
        return result;
    }

    virtual void OnBlobFree(const TUnifiedBlobId& blobId) override;

    const NColumnShard::TBlobsManagerCounters& GetCounters() const {
        return BlobsManagerCounters;
    }

    TTabletId GetSelfTabletId() const {
        return SelfTabletId;
    }

    ui64 GetTabletId() const {
        return TabletInfo->TabletID;
    }

    ui64 GetCurrentGen() const {
        return CurrentGen;
    }

    void RegisterControls(NKikimr::TControlBoard& icb);

    // Loads the state at startup
    bool LoadState(IBlobManagerDb& db, const TTabletId selfTabletId);

    // Prepares Keep/DontKeep lists and GC barrier
    std::shared_ptr<NBlobOperations::NBlobStorage::TGCTask> BuildGCTask(const TString& storageId,
        const std::shared_ptr<TBlobManager>& manager, const std::shared_ptr<NDataSharing::TStorageSharedBlobsManager>& sharedBlobsInfo,
        const std::shared_ptr<NBlobOperations::TRemoveGCCounters>& counters) noexcept;

    void OnGCFinishedOnExecute(const std::optional<TGenStep>& genStep, IBlobManagerDb& db);
    void OnGCFinishedOnComplete(const std::optional<TGenStep>& genStep);

    void OnGCStartOnExecute(const std::optional<TGenStep>& genStep, IBlobManagerDb& db);
    void OnGCStartOnComplete(const std::optional<TGenStep>& genStep);

    TBlobManagerCounters GetCountersUpdate() {
        TBlobManagerCounters res = CountersUpdate;
        CountersUpdate = TBlobManagerCounters();
        return res;
    }

    // Implementation of IBlobManager interface
    TBlobBatch StartBlobBatch() override;
    virtual void DeleteBlobOnExecute(const TTabletId tabletId, const TUnifiedBlobId& blobId, IBlobManagerDb& db) override;
    virtual void DeleteBlobOnComplete(const TTabletId tabletId, const TUnifiedBlobId& blobId) override;
private:
    std::deque<TGenStep> FindNewGCBarriers();
    void PopGCBarriers(const TGenStep gs);
    void PopGCBarriers(const ui32 count);

    bool ExtractEvicted(TEvictedBlob& evict, TEvictMetadata& meta, bool fromDropped = false);

    TGenStep EdgeGenStep() const {
        return CollectGenStepInFlight ? *CollectGenStepInFlight : std::max(GCBarrierPreparation, LastCollectedGenStep);
    }
};

}
