#pragma once

#include <util/system/types.h>

namespace NKikimr::NDDisk {

struct TDDiskConfig {
    bool ForcePDiskFallback = false;
    bool EnableChecksums = true;
    // Bounds the memory TIntegrityManager spends on cached data block checksums / digests
    // (see the memory note in integrity_manager.h). Must match
    // TIntegrityManager::DefaultChecksumCacheBytes by default.
    ui64 IntegrityChecksumCacheBytes = 64ull << 20;
};

} // NKikimr::NDDisk
