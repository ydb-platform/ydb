#pragma once

#include <util/system/types.h>

namespace NKikimr::NDDisk {

struct TDDiskConfig {
    bool UseSQPoll = false;
    bool UseIOPoll = false;
    bool ForcePDiskFallback = false;
    bool EnableChecksums = true;

    // When EnableChecksums is true, recompute payload checksums and reject a write
    // with CORRUPTED if they do not match the sender-supplied list. Default off:
    // checksums are still required and stored, but not verified on the way in.
    bool CheckChecksumBeforeWrite = false;

    // When EnableChecksums is true, recompute payload checksums of data read from
    // disk and reject with CORRUPTED on mismatch. In-memory PersistentBuffer hits
    // and synthetic DDisk zeros are not checked.
    bool CheckChecksumWhenRead = false;

    // Bounds the memory TIntegrityManager spends on cached data block checksums / digests
    // (see the memory note in integrity_manager.h). Must match
    // TIntegrityManager::DefaultChecksumCacheBytes by default.
    ui64 IntegrityChecksumCacheBytes = 64ull << 20;
};

} // NKikimr::NDDisk
