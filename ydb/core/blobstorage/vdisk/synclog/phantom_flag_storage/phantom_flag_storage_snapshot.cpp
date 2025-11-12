#include "phantom_flag_storage_snapshot.h"

namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageSnapshot::TPhantomFlagStorageSnapshot(const TPhantomFlags& flags)
    : Flags(flags)
{}

} // namespace NSyncLog

} // namespace NKikimr
