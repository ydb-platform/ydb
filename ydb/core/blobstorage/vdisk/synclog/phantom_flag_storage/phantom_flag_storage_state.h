#pragma once

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>

#include "phantom_flags.h"
#include "phantom_flag_storage_data.h"
#include "phantom_flag_storage_snapshot.h"
#include "phantom_flag_thresholds.h"

#include <vector>

namespace NKikimr {

namespace NSyncLog {

// Manages Phantom Flag Storage - in-memory (TODO: persistent) storage for DoNotKeep flags
// Stores DoNotKeeps from cut synclog chunks which weren't synced with some VDisks of storage group

class TPhantomFlagStorageState {
public:
    TPhantomFlagStorageState(TIntrusivePtr<TSyncLogCtx> slCtx);

    void InitializePersistent(TPhantomFlagStorageData&& data, TActorId syncLogKeeperId,
            TActorId chunkKeeperId, ui32 appendBlockSize);
    void StartBuilding();

    // Adds DoNotKeep flags from synclog if needed
    void ProcessBlobRecordFromSyncLog(const TLogoBlobRec* blobRec, ui64 sizeLimit);

    // Add all DoNotKeep records from cut synclog snapshot up to sizeLimit
    // Note: in some obscure cases there may be two active builders simultaneously
    // It shouldn't make any difference though, we just add more flags
    void FinishInitialBuilding(TPhantomFlags&& flags, TPhantomFlagThresholds&& thresholds, ui64 sizeLimit);
    void Recover(TPhantomFlagStorageSnapshot&& snapshot);
    void Deactivate();

    // Read everything from storage
    void RequestSnapshot(TEvPhantomFlagStorageGetSnapshot::TPtr request) const;
    bool IsActive() const;

    // Process sync data from neighbours, we do it to update Thresholds
    void ProcessLocalSyncData(ui32 orderNumber, const TString& data);

    ui64 EstimateFlagsMemoryConsumption() const;
    ui64 EstimateThresholdsMemoryConsumption() const;

    void UpdateSyncedMask(const TSyncedMask& newSyncedMask);

    void UpdateMetrics();

    std::optional<TPhantomFlagStorageData> GetPersistentData() const;
    void UpdatePersistentData(std::optional<TPhantomFlagStorageData>&& data);
    void FlushWriteBufferIfNeeded();
    void SyncLogIsCut();
    void Terminate();

private:
    // Adds DoNotKeep flags to storage and Keeps to Thresholds for specified neighbour
    void ProcessBlobRecordFromNeighbour(ui32 orderNumber, const TLogoBlobRec* blobRec);
    // Prune Thresholds
    void ProcessBarrierRecordFromNeighbour(ui32 orderNumber, const TBarrierRec* barrierRec);

    void AdjustSize(ui64 sizeLimit);
    bool AddFlag(const TLogoBlobRec& blobRec);

    void AddItemToWriteBuffer(const TPhantomFlagStorageItem& item);
    void FlushWriteBuffer();

private:
    TIntrusivePtr<TSyncLogCtx> SlCtx;
    const TBlobStorageGroupType GType;
    TPhantomFlagThresholds Thresholds;
    TPhantomFlags StoredFlags;
    ui64 MaxFlagsStoredCount = 0;
    TSyncedMask SyncedMask;
    bool Active = false;
    bool Building = false;

    // persistent phantom flag storage
    bool IsPersistent = false;
    TActorId ProcessorId;
    std::vector<TPhantomFlagStorageItem> WriteBuffer;
    ui32 WriteBufferSize = 0;
    std::optional<TPhantomFlagStorageData> PersistentData;
    TMonotonic WriteBufferFlushTimestamp = TMonotonic::Zero();

private:
    // TODO: remove write buffer, use sync log
    constexpr static ui32 WriteBufferSizeLimit = 1_MB;
    constexpr static TDuration WriteBufferFlushPeriod = TDuration::Seconds(30);
};

} // namespace NSyncLog

} // namespace NKikimr
