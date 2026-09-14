#include "phantom_flag_storage_snapshot.h"

namespace NKikimr {

namespace NSyncLog {

TPhantomFlagStorageSnapshot::TPhantomFlagStorageSnapshot(TPhantomFlags&& flags,
            TPhantomFlagThresholds&& thresholds)
    : Flags(std::move(flags))
    , Thresholds(std::move(thresholds))
{}

TPhantomFlagStorageSnapshot::TPhantomFlagStorageSnapshot(const TPhantomFlags& flags,
            const TPhantomFlagThresholds& thresholds)
    : Flags(flags)
    , Thresholds(thresholds)
{}

} // namespace NSyncLog

} // namespace NKikimr
