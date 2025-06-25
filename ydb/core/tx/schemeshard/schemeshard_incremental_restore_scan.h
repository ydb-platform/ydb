#pragma once

#include "schemeshard_private.h"

namespace NKikimr::NSchemeShard::NIncrementalRestoreScan {

// Transaction types for incremental restore scan
constexpr ui32 TXTYPE_PROGRESS_INCREMENTAL_RESTORE = 1100; // Using a unique ID

} // namespace NKikimr::NSchemeShard::NIncrementalRestoreScan
