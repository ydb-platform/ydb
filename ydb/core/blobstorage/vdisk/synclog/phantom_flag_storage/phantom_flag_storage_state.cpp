#include "phantom_flag_storage_state.h"

namespace NKikimr {

namespace NSyncLog {

void TPhantomFlagStorageState::Activate() {
    Active = true;
}

void TPhantomFlagStorageState::AddFlags(TPhantomFlags flags) {
    StoredFlags.insert(StoredFlags.end(), flags.begin(), flags.end());
}

void TPhantomFlagStorageState::Clear() {
    StoredFlags.clear();
}

TPhantomFlags TPhantomFlagStorageState::GetFlags() const {
    return StoredFlags;
}

bool TPhantomFlagStorageState::IsActive() const {
    return Active;
}

} // namespace NSyncLog

} // namespace NKikimr
