#pragma once

#include "phantom_flags.h"
#include "phantom_flag_thresholds.h"

namespace NKikimr {

namespace NSyncLog {

struct TPhantomFlagStorageSnapshot {
    TPhantomFlagStorageSnapshot(TPhantomFlags&& flags,
            TPhantomFlagThresholds&& thresholds);

    TPhantomFlagStorageSnapshot(const TPhantomFlags& flags,
            const TPhantomFlagThresholds& thresholds);

    TPhantomFlags Flags;
    TPhantomFlagThresholds Thresholds;
};

} // namespace NSyncLog

} // namespace NKikimr
