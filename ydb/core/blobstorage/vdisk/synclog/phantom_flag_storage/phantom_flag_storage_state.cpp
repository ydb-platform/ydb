#include "phantom_flag_storage_state.h"

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageState::TPhantomFlagStorageState(TIntrusivePtr<TSyncLogCtx> slCtx)
    : SlCtx(slCtx)
    , GType(slCtx->VCtx->Top->GType)
    , Thresholds(GType)
{}

void TPhantomFlagStorageState::StartBuilding() {
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
        AddFlag(*blobRec);
    }
}

void TPhantomFlagStorageState::ProcessBlobRecordFromNeighbour(ui32 orderNumber, const TLogoBlobRec* blobRec) {
    Thresholds.AddBlob(orderNumber, blobRec->LogoBlobID());
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
    for (const TLogoBlobRec& rec : flags) {
        bool added = AddFlag(rec);
        if (!added) {
            break;
        }
    }
    Thresholds.Merge(std::move(thresholds));

    Building = false;
}

void TPhantomFlagStorageState::Deactivate() {
    StoredFlags.clear();
    Active = false;
    Building = false;
}

TPhantomFlagStorageSnapshot TPhantomFlagStorageState::GetSnapshot() const {
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
    if (newCapacity > StoredFlags.capacity()) {
        StoredFlags.reserve(newCapacity);
    } else if (newCapacity < StoredFlags.capacity()) {
        if (newCapacity < StoredFlags.size()) {
            StoredFlags = TPhantomFlags(StoredFlags.begin(), StoredFlags.begin() + newCapacity);
        }
        StoredFlags.shrink_to_fit();
        StoredFlags.reserve(newCapacity);
    }
}

bool TPhantomFlagStorageState::AddFlag(const TLogoBlobRec& blobRec) {
    if (StoredFlags.size() < StoredFlags.capacity()) {
        StoredFlags.emplace_back(blobRec);
        return true;
    }
    return false;
}

void TPhantomFlagStorageState::UpdateSyncedMask(TSyncedMask newSyncedMask) {
    SyncedMask = newSyncedMask;
}

} // namespace NSyncLog

} // namespace NKikimr
