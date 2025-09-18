#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

#include "blobstorage_blob.h"

namespace NKikimr {

    ui64 TDiskBlob::CalculateChecksum(const TRope& rope, size_t numBytes) {
        XXH3_state_t state;
        XXH3_64bits_reset(&state);

        for (auto it = rope.Begin(); numBytes && it.Valid(); it.AdvanceToNextContiguousBlock()) {
            const size_t n = Min(numBytes, it.ContiguousSize());
            XXH3_64bits_update(&state, it.ContiguousData(), n);
            numBytes -= n;
        }

        return XXH3_64bits_digest(&state);
    }

    bool TDiskBlob::ValidateChecksum(const TRope& rope) {
        ui64 checksum;
        size_t numBytes = rope.size() - sizeof(checksum);

        XXH3_state_t state;
        XXH3_64bits_reset(&state);

        for (auto it = rope.Begin(); it.Valid(); it.AdvanceToNextContiguousBlock()) {
            const size_t n = Min(numBytes, it.ContiguousSize());
            XXH3_64bits_update(&state, it.ContiguousData(), n);
            numBytes -= n;
            if (!numBytes) {
                it += n;
                it.ExtractPlainDataAndAdvance(&checksum, sizeof(checksum));
                return checksum == XXH3_64bits_digest(&state);
            }
        }

        Y_ABORT();
    }

} // NKikimr
