#include "ddisk_checksums.h"

#include <util/generic/utility.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

#include <array>
#include <cstddef>

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

ui64 CalculateRawChecksum(const void* data, size_t size) {
    return XXH3_64bits(data, size);
}

namespace {

// XXH3_64 over [data, data+size) with [fieldOffset, fieldOffset+fieldSize) hashed as zeros.
// Used so integrity-block self-checksums can be verified without copying the 4 KiB block.
ui64 CalculateRawChecksumZeroedField(const void* data, size_t size, size_t fieldOffset, size_t fieldSize) {
    Y_ABORT_UNLESS(fieldOffset + fieldSize <= size);
    const char* bytes = static_cast<const char*>(data);

    XXH3_state_t state;
    XXH3_64bits_reset(&state);
    if (fieldOffset) {
        XXH3_64bits_update(&state, bytes, fieldOffset);
    }
    ui8 zeros[16] = {};
    Y_ABORT_UNLESS(fieldSize <= sizeof(zeros));
    XXH3_64bits_update(&state, zeros, fieldSize);
    const size_t tailOffset = fieldOffset + fieldSize;
    if (tailOffset < size) {
        XXH3_64bits_update(&state, bytes + tailOffset, size - tailOffset);
    }
    return XXH3_64bits_digest(&state);
}

} // anonymous

ui64 Contribution(ui64 vchunkGeneration, ui64 blockIdx, ui64 blockChecksum) {
    ui64 parts[3] = {vchunkGeneration, blockIdx, blockChecksum};
    return XXH3_64bits(parts, sizeof(parts));
}

void UpdateRoot(ui64& integrityBlockDigest, ui64 vchunkGeneration, ui64 idx, ui64 oldCsum, ui64 newCsum) {
    integrityBlockDigest ^= Contribution(vchunkGeneration, idx, oldCsum);
    integrityBlockDigest ^= Contribution(vchunkGeneration, idx, newCsum);
}

ui64 CalculateChecksumIdentitySalt(ui64 ddiskId, ui64 pdiskGuid, ui64 tabletId,
        ui64 vChunkIndex, ui64 blockIdx) {
    const ui64 parts[] = {ddiskId, pdiskGuid, tabletId, vChunkIndex, blockIdx};
    return XXH3_64bits(parts, sizeof(parts));
}

ui64 SealBlockChecksum(ui64 pureChecksum, ui64 ddiskId, ui64 pdiskGuid, ui64 tabletId,
        ui64 vChunkIndex, ui64 blockIdx) {
    return pureChecksum
        ^ CalculateChecksumIdentitySalt(ddiskId, pdiskGuid, tabletId, vChunkIndex, blockIdx);
}

ui64 UnsealBlockChecksum(ui64 storedChecksum, ui64 ddiskId, ui64 pdiskGuid, ui64 tabletId,
        ui64 vChunkIndex, ui64 blockIdx) {
    return SealBlockChecksum(storedChecksum, ddiskId, pdiskGuid, tabletId, vChunkIndex, blockIdx);
}

ui64 GetZeroBlockChecksum() {
    static const ui64 checksum = [] {
        const std::array<ui8, IntegrityUnitSize> zero{};
        return XXH3_64bits(zero.data(), zero.size());
    }();
    return checksum;
}

bool ValidateIntegrityBlock(const TIntegrityBlock& block, const TIntegrityBlockIdentity& expected) {
    const ui64 expectedChecksum = CalculateRawChecksumZeroedField(
        &block, sizeof(block), offsetof(TIntegrityBlockHeader, BlockChecksum), sizeof(block.Header.BlockChecksum));
    if (block.Header.BlockChecksum != expectedChecksum) {
        return false;
    }

    const TIntegrityBlockHeader& header = block.Header;
    return header.Magic == MagicIntegrityBlock
        && header.FormatVersion == static_cast<ui16>(EIntegrityFormatVersion::BaseAwupf4KiB)
        && header.ChecksumBlockIdx == expected.ChecksumBlockIdx
        && header.OwnerId == expected.OwnerId
        && header.VChunkId == expected.VChunkId
        && header.VChunkGeneration == expected.VChunkGeneration
        && header.IntegrityChunkId == expected.IntegrityChunkId
        && header.IntegrityExtentId == expected.IntegrityExtentId
        && header.IntegrityChunkGeneration == expected.IntegrityChunkGeneration;
}

i32 SelectIntegrityBlockWinner(const TIntegrityBlock (&slots)[IntegrityPairSlots],
        const TIntegrityBlockIdentity& expected) {
    const bool validA = ValidateIntegrityBlock(slots[0], expected);
    const bool validB = ValidateIntegrityBlock(slots[1], expected);
    if (!validA) {
        return validB ? 1 : -1;
    }
    if (!validB) {
        return 0;
    }
    return slots[1].Header.PairSequenceNumber >= slots[0].Header.PairSequenceNumber ? 1 : 0;
}

} // namespace NKikimr::NDDisk
