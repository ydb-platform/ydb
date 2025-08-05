#include "phantom_flag_storage_state.h"

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>

namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageState::TPhantomFlagStorageState(const TBlobStorageGroupType& gtype)
    : GType(gtype)
    , Thresholds(GType)
{}

void TPhantomFlagStorageState::Activate() {
    Active = true;
}

void TPhantomFlagStorageState::ProcessBlobRecordFromSyncLog(const TLogoBlobRec* blobRec) {
    if (blobRec->Ingress.IsDoNotKeep(GType)) {
        if (Thresholds.IsBehindThreshold(blobRec->LogoBlobID())) {
            StoredFlags.push_back(*blobRec);
        }
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

void TPhantomFlagStorageState::AddFlags(TPhantomFlags flags) {
    StoredFlags.insert(StoredFlags.end(), flags.begin(), flags.end());
}

void TPhantomFlagStorageState::Deactivate() {
    StoredFlags.clear();
    Active = false;
}

TPhantomFlagStorageSnapshot TPhantomFlagStorageState::GetSnapshot() const {
    return TPhantomFlagStorageSnapshot(StoredFlags);
}

bool TPhantomFlagStorageState::IsActive() const {
    return Active;
}

void TPhantomFlagStorageState::ProcessLocalSyncData(ui32 orderNumber, const TString& data) {
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

} // namespace NSyncLog

} // namespace NKikimr
