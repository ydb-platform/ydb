#include "ddisk.h"

namespace NKikimr::NDDisk {

namespace {

// AddPayloadThenChecksum() always checksums payload 0; make sure the write instruction agrees, so a sender
// that ever passes a non-zero PayloadId does not end up with a checksum silently computed over the
// wrong payload (which would look like every write is corrupted).
void CheckInstructionPointsAtPayloadZero(const NKikimrBlobStorage::NDDisk::TWriteInstruction& instructionPb) {
    const TWriteInstruction instr(instructionPb);
    Y_ABORT_UNLESS(instr.PayloadId && *instr.PayloadId == 0,
        "AddPayloadThenChecksum assumes the write instruction points at payload 0");
}

template <typename TEvent>
ui32 AddPayloadThenChecksumImpl(TEvent& ev, TRope&& rope) {
    const ui32 id = ev.AddPayload(std::move(rope));
    Y_ABORT_UNLESS(ev.GetPayloadCount() > 0);
    CheckInstructionPointsAtPayloadZero(ev.Record.GetInstruction());

    ev.Record.ClearChecksums();
    for (ui64 checksum : CalculatePayloadChecksums(ev.GetPayload(0))) {
        ev.Record.AddChecksums(checksum);
    }
    return id;
}

} // anonymous

ui32 TEvWrite::AddPayloadThenChecksum(TRope&& rope) {
    return AddPayloadThenChecksumImpl(*this, std::move(rope));
}

ui32 TEvWritePersistentBuffer::AddPayloadThenChecksum(TRope&& rope) {
    return AddPayloadThenChecksumImpl(*this, std::move(rope));
}

ui32 TEvWritePersistentBuffers::AddPayloadThenChecksum(TRope&& rope) {
    return AddPayloadThenChecksumImpl(*this, std::move(rope));
}

} // namespace NKikimr::NDDisk
