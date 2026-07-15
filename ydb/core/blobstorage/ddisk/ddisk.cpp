#include "ddisk.h"

#include <util/generic/utility.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr::NDDisk {

namespace {

// ChecksumPayload() always checksums payload 0; make sure the write instruction agrees, so a sender
// that ever passes a non-zero PayloadId does not end up with a checksum silently computed over the
// wrong payload (which would look like every write is corrupted).
void CheckInstructionPointsAtPayloadZero(const NKikimrBlobStorage::NDDisk::TWriteInstruction& instructionPb) {
    const TWriteInstruction instr(instructionPb);
    Y_ABORT_UNLESS(instr.PayloadId && *instr.PayloadId == 0,
        "ChecksumPayload assumes the write instruction points at payload 0");
}

} // anonymous

ui64 CalculateBlockChecksum(TRope::TConstIterator it, size_t numBytes) {
    Y_ABORT_UNLESS(numBytes > 0);
    Y_ABORT_UNLESS((numBytes & (MinSectorSize - 1)) == 0);

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
    Y_ABORT_UNLESS((payload.size() & (MinSectorSize - 1)) == 0);

    std::vector<ui64> checksums;
    checksums.reserve(payload.size() / MinSectorSize);

    auto it = payload.Begin();
    for (size_t offset = 0; offset < payload.size(); offset += MinSectorSize) {
        checksums.push_back(CalculateBlockChecksum(it, MinSectorSize));
        it += MinSectorSize;
    }

    return checksums;
}

void TEvWrite::ChecksumPayload() {
    Y_ABORT_UNLESS(GetPayloadCount() > 0);
    CheckInstructionPointsAtPayloadZero(Record.GetInstruction());

    Record.ClearChecksums();
    AddChecksum(CalculatePayloadChecksums(GetPayload(0)));
}

void TEvWritePersistentBuffer::ChecksumPayload() {
    Y_ABORT_UNLESS(GetPayloadCount() > 0);
    CheckInstructionPointsAtPayloadZero(Record.GetInstruction());

    Record.ClearChecksums();
    AddChecksum(CalculatePayloadChecksums(GetPayload(0)));
}

void TEvWritePersistentBuffers::ChecksumPayload() {
    Y_ABORT_UNLESS(GetPayloadCount() > 0);
    CheckInstructionPointsAtPayloadZero(Record.GetInstruction());

    Record.ClearChecksums();
    AddChecksum(CalculatePayloadChecksums(GetPayload(0)));
}

} // namespace NKikimr::NDDisk
