#include "ddisk_checksums.h"

#include <util/generic/utility.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NDDisk {

ui64 CalculateBlockChecksum(TRope::TConstIterator it, size_t numBytes) {
    Y_ABORT_UNLESS(numBytes > 0);
    Y_ABORT_UNLESS((numBytes & (IntegrityUnitSize - 1)) == 0);

    // Fast path: the block is fully contiguous (always true for TEvWrite payloads, and common for PB
    // ones), so a single one-shot hash call avoids the XXH3 streaming state overhead.
    if (it.Valid() && it.ContiguousSize() >= numBytes) {
        return XXH3_64bits(it.ContiguousData(), numBytes);
    }

    XXH3_state_t state;
    XXH3_64bits_reset(&state);
    for (; numBytes && it.Valid(); it.AdvanceToNextContiguousBlock()) {
        const size_t n = Min(numBytes, it.ContiguousSize());
        XXH3_64bits_update(&state, it.ContiguousData(), n);
        numBytes -= n;
    }
    // The iterator must not run out before numBytes bytes are consumed: otherwise the digest would
    // silently cover fewer bytes than the caller asked for, which is worse than crashing.
    Y_ABORT_UNLESS(numBytes == 0);
    return XXH3_64bits_digest(&state);
}

std::vector<ui64> CalculatePayloadChecksums(const TRope& payload) {
    Y_ABORT_UNLESS(payload.size() > 0);
    Y_ABORT_UNLESS((payload.size() & (IntegrityUnitSize - 1)) == 0);

    std::vector<ui64> checksums;
    checksums.reserve(payload.size() / IntegrityUnitSize);

    auto it = payload.Begin();
    for (size_t offset = 0; offset < payload.size(); offset += IntegrityUnitSize) {
        checksums.push_back(CalculateBlockChecksum(it, IntegrityUnitSize));
        it += IntegrityUnitSize;
    }

    return checksums;
}

ui64 Contribution(ui64 vchunkGeneration, ui64 blockIdx, ui64 blockChecksum) {
    ui64 parts[3] = {vchunkGeneration, blockIdx, blockChecksum};
    return XXH3_64bits(parts, sizeof(parts));
}

void UpdateRoot(ui64& integrityBlockDigest, ui64 vchunkGeneration, ui64 idx, ui64 oldCsum, ui64 newCsum) {
    integrityBlockDigest ^= Contribution(vchunkGeneration, idx, oldCsum);
    integrityBlockDigest ^= Contribution(vchunkGeneration, idx, newCsum);
}

} // namespace NKikimr::NDDisk
