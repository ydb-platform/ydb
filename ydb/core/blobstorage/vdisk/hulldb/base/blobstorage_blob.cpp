#include "blobstorage_blob.h"

#include <ydb/core/blobstorage/base/blobstorage_checksum.h>

namespace NKikimr {

    ui64 TDiskBlob::CalculateChecksum(const TRope& rope, size_t numBytes) {
        return CalculateXxh3Hash(rope.Begin(), numBytes).second;
    }

    bool TDiskBlob::ValidateChecksum(const TRope& rope) {
        Y_ABORT_UNLESS(rope.size() >= sizeof(ui64));
        size_t payloadSize = rope.size() - sizeof(ui64);
        auto [it, calculatedChecksum] = CalculateXxh3Hash(rope.Begin(), payloadSize);

        ui64 storedChecksum;
        it.ExtractPlainDataAndAdvance(&storedChecksum, sizeof(storedChecksum));
        return storedChecksum == calculatedChecksum;
    }

} // NKikimr
