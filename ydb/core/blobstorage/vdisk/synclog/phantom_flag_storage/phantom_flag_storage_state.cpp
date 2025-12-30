#include "phantom_flag_storage_state.h"

#include <ydb/core/util/stlog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageState::TPhantomFlagStorageState(TIntrusivePtr<TSyncLogCtx> slCtx)
    : SlCtx(slCtx)
    , GType(slCtx->VCtx->Top->GType)
    , Thresholds(GType)
    , MaxFlagsStoredCount(0)
{}

void TPhantomFlagStorageState::StartBuilding() {
    if (GType.BlobSubgroupSize() > MaxExpectedDisksInGroup) {
        STLOG(PRI_ERROR, BS_PHANTOM_FLAG_STORAGE, BSPFS01,
                VDISKP(SlCtx->VCtx, "Attempted to start phantom flag storage building on unsupported configuration"),
                (MaxExpectedDisksInGroup, MaxExpectedDisksInGroup),
                (GroupSize, GType.BlobSubgroupSize()));
        // PhantomFlagStorage doesn't work with weird group configurations to minimize memory consumption
        return;
    }
    Active = true;
    Building = true;
}

void TPhantomFlagStorageState::ProcessBlobRecordFromSyncLog(const TLogoBlobRec* blobRec, ui64 sizeLimit) {
    AdjustSize(sizeLimit);
    if (!Active) {
        return;
    }

    if (blobRec->Ingress.IsDoNotKeep(GType) &&
            (Building || Thresholds.IsBehindThresholdOnUnsynced(blobRec->LogoBlobID(), SyncedMask))) {
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS09,
                VDISKP(SlCtx->VCtx, "Try to add DoNotKeepFlag flag to PhantomFlagStorage"),
                (BlobId, blobRec->LogoBlobID().ToString()),
                (Building, Building),
                (SyncedMask, SyncedMask.to_ullong()),
                (Thresholds, Thresholds.ToString()));
        AddFlag(*blobRec);
    }
}

void TPhantomFlagStorageState::ProcessBlobRecordFromNeighbour(ui32 orderNumber, const TLogoBlobRec* blobRec) {
    if (blobRec->Ingress.IsKeep(GType)) {
        Thresholds.AddBlob(orderNumber, blobRec->LogoBlobID());
    }
}

void TPhantomFlagStorageState::ProcessBarrierRecordFromNeighbour(ui32 orderNumber, const TBarrierRec* barrierRec) {
    if (barrierRec->Hard) {
        Thresholds.AddHardBarrier(orderNumber, barrierRec->TabletId, barrierRec->Channel,
                barrierRec->CollectGeneration, barrierRec->CollectStep);
    }
}

void TPhantomFlagStorageState::FinishBuilding(TPhantomFlags&& flags, TPhantomFlagThresholds&& thresholds,
        ui64 sizeLimit) {
    if (!Active) {
        // PhantomFlagStorage was deactivated while building, do nothing
        return;
    }

    AdjustSize(sizeLimit);
    ui64 flagsAdded = 0;
    for (const TLogoBlobRec& rec : flags) {
        if (!AddFlag(rec)) {
            break;
        }
        ++flagsAdded;
    }
    Thresholds.Merge(std::move(thresholds));

    Building = false;

    STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS06,
            VDISKP(SlCtx->VCtx, "Finish building"),
            (FlagsAdded, flagsAdded),
            (FlagsReceived, flags.size()));
}

void TPhantomFlagStorageState::Deactivate() {
    STLOG(PRI_NOTICE, BS_PHANTOM_FLAG_STORAGE, BSPFS07,
            VDISKP(SlCtx->VCtx, "Deactivating PhantomFlagStorage"),
            (FlagsDropped, StoredFlags.size()));
    StoredFlags.clear();
    Thresholds.Clear();
    Active = false;
    Building = false;
}

TPhantomFlagStorageSnapshot TPhantomFlagStorageState::GetSnapshot() const {
    STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS05,
            VDISKP(SlCtx->VCtx, "Acquiring snapshot"),
            (FlagsCount, StoredFlags.size()));
    return TPhantomFlagStorageSnapshot(StoredFlags);
}

bool TPhantomFlagStorageState::IsActive() const {
    return Active;
}

void TPhantomFlagStorageState::ProcessLocalSyncData(ui32 orderNumber, const TString& data) {
    if (!Active) {
        return;
    }

    auto blobHandler = [&] (const NSyncLog::TLogoBlobRec* rec) {
        ProcessBlobRecordFromNeighbour(orderNumber, rec);
    };
    auto blockHandler = [&] (const NSyncLog::TBlockRec*) {
        // nothing to do
    };
    auto barrierHandler = [&] (const NSyncLog::TBarrierRec* rec) {
        ProcessBarrierRecordFromNeighbour(orderNumber, rec);
    };
    auto blockHandlerV2 = [&](const NSyncLog::TBlockRecV2*) {
        // nothing to do
    };

    // process synclog data
    NSyncLog::TFragmentReader fragment(data);
    fragment.ForEach(blobHandler, blockHandler, barrierHandler, blockHandlerV2);
}


ui64 TPhantomFlagStorageState::EstimateFlagsMemoryConsumption() const {
    return StoredFlags.capacity() * sizeof(decltype(StoredFlags)::value_type);
}

ui64 TPhantomFlagStorageState::EstimateThresholdsMemoryConsumption() const {
    return Thresholds.EstimatedMemoryConsumption();
}

void TPhantomFlagStorageState::AdjustSize(ui64 sizeLimit) {
    ui32 newCapacity = sizeLimit / sizeof(decltype(StoredFlags)::value_type);
    if (newCapacity > MaxFlagsStoredCount) {
        StoredFlags.reserve(newCapacity);
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS03,
                VDISKP(SlCtx->VCtx, "Reserving additional space for PhantomFlagStorage"),
                (OldCapacity, MaxFlagsStoredCount),
                (NewCapacity, newCapacity),
                (ActualCapacity, StoredFlags.capacity()));
    } else if (newCapacity < MaxFlagsStoredCount) {
        ui32 flagsDropped = 0;
        if (newCapacity < StoredFlags.size()) {
            flagsDropped = StoredFlags.size() - newCapacity;
            StoredFlags = TPhantomFlags(StoredFlags.begin(), StoredFlags.begin() + newCapacity);
        }
        StoredFlags.shrink_to_fit();
        StoredFlags.reserve(newCapacity);
        STLOG(PRI_DEBUG, BS_PHANTOM_FLAG_STORAGE, BSPFS04,
                VDISKP(SlCtx->VCtx, "Shrinking PhantomFlagStorage"),
                (OldCapacity, MaxFlagsStoredCount),
                (NewCapacity, newCapacity),
                (ActualCapacity, StoredFlags.capacity()),
                (FlagsDropped, flagsDropped));
    }
    MaxFlagsStoredCount = newCapacity;
}

bool TPhantomFlagStorageState::AddFlag(const TLogoBlobRec& blobRec) {
    if (StoredFlags.size() < StoredFlags.capacity()) {
        StoredFlags.emplace_back(blobRec);
        return true;
    } else {
        STLOG(PRI_INFO, BS_PHANTOM_FLAG_STORAGE, BSPFS02,
                VDISKP(SlCtx->VCtx, "Cannot add flag to PhantomFlagStorage, memory limit reached"),
                (Capacity, StoredFlags.capacity()),
                (Size, StoredFlags.size()),
                (BlobId, blobRec.LogoBlobID().ToString()));
        return false;
    }
}

void TPhantomFlagStorageState::UpdateSyncedMask(const TSyncedMask& newSyncedMask) {
    SyncedMask = newSyncedMask;
}

void TPhantomFlagStorageState::UpdateMetrics() {
    SlCtx->PhantomFlagStorageGroup.IsPhantomFlagStorageActive() = Active;
    SlCtx->PhantomFlagStorageGroup.IsPhantomFlagStorageBuilding() = Building;
    SlCtx->PhantomFlagStorageGroup.StoredFlagsCount() = StoredFlags.size();
    ui64 storedFlagsMem = StoredFlags.capacity() * sizeof(decltype(StoredFlags)::value_type);
    SlCtx->PhantomFlagStorageGroup.StoredFlagsMemoryConsumption() = storedFlagsMem;
    SlCtx->PhantomFlagStorageGroup.ThresholdsMemoryConsumption() = Thresholds.EstimatedMemoryConsumption();
}

} // namespace NSyncLog

} // namespace NKikimr
