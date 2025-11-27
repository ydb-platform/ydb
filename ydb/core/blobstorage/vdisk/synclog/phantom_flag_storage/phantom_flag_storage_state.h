#pragma once

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>

#include "phantom_flags.h"
#include "phantom_flag_storage_snapshot.h"
#include "phantom_flag_thresholds.h"

#include <vector>

namespace NKikimr {

namespace NSyncLog {

// Manages Phantom Flag Storage - in-memory (TODO: persistent) storage for DoNotKeep flags
// Stores DoNotKeeps from cut synclog chunks which weren't synced with some VDisks of storage group

class TPhantomFlagStorageState {
public:
    TPhantomFlagStorageState(const TBlobStorageGroupType& gtype);

    void Activate();

    void ProcessBlobRecordFromSyncLog(const TLogoBlobRec* blobRec, ui64 sizeLimit);
    // Add all DoNotKeep records from cut synclog snapshot up to sizeLimit
    void AddFlags(TPhantomFlags flags, ui64 sizeLimit);
    void Deactivate();

    // TODO: rebuild thresholds structure after restart. Either write it to VDisk log or rebuild from hull snapshot

    // Read everything from storage
    TPhantomFlagStorageSnapshot GetSnapshot() const;
    bool IsActive() const;

    void ProcessLocalSyncData(ui32 orderNumber, const TString& data);

    ui64 EstimateFlagsMemoryConsumption() const;
    ui64 EstimateThresholdsMemoryConsumption() const;

private:
    // Adds DoNotKeep flags to storage and Keeps to Thresholds for specified neighbour
    void ProcessBlobRecordFromNeighbour(ui32 orderNumber, const TLogoBlobRec* blobRec);
    // Prune Thresholds
    void ProcessBarrierRecordFromNeighbour(ui32 orderNumber, const TBarrierRec* barrierRec);

    void AdjustSize(ui64 sizeLimit);
    bool AddFlag(const TLogoBlobRec& blobRec);

private:
    const TBlobStorageGroupType GType;
    TPhantomFlagThresholds Thresholds;
    TPhantomFlags StoredFlags;
    bool Active = false;
};

} // namespace NSyncLog

} // namespace NKikimr
