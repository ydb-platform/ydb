#pragma once

#include "phantom_flags.h"
#include "phantom_flag_thresholds.h"

namespace NKikimr {

namespace NSyncLog {

// TODO: include thresholds in snapshot
struct TPhantomFlagStorageSnapshot {
public:
    TPhantomFlagStorageSnapshot(const TPhantomFlags& flags);
    TPhantomFlags Flags;
};

} // namespace NSyncLog

} // namespace NKikimr
