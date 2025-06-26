#include "phantom_flag_storage_state.h"

namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageState::TPhantomFlagStorageState(const TBlobStorageGroupType& gtype)
    : GType(gtype)
    , Thresholds(GType)
{}

void TPhantomFlagStorageState::Activate() {
    Active = true;
}

void TPhantomFlagStorageState::AddBlobRecord(const TLogoBlobRec& blobRec) {
    if (blobRec.Ingress.IsDoNotKeep(GType)) {
        // TODO: use threshold structure
        if (true || Thresholds.IsBehindThreshold(blobRec.LogoBlobID())) {
            StoredFlags.push_back(blobRec);
        }
    }
}

void TPhantomFlagStorageState::AddFlags(TPhantomFlags flags) {
    StoredFlags.insert(StoredFlags.end(), flags.begin(), flags.end());
}

void TPhantomFlagStorageState::Clear() {
    StoredFlags.clear();
}

TPhantomFlagStorageSnapshot TPhantomFlagStorageState::GetSnapshot() const {
    return TPhantomFlagStorageSnapshot(StoredFlags);
}

bool TPhantomFlagStorageState::IsActive() const {
    return Active;
}

} // namespace NSyncLog

} // namespace NKikimr
