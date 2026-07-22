#include "blobstorage_checksum.h"

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr {

std::pair<TRope::TConstIterator, ui64> CalculateXxh3Hash(TRope::TConstIterator it, size_t numBytes) {
    XXH3_state_t state;
    XXH3_64bits_reset(&state);

    while (numBytes && it.Valid()) {
        const size_t n = Min(numBytes, it.ContiguousSize());
        XXH3_64bits_update(&state, it.ContiguousData(), n);
        numBytes -= n;
        it += n;
    }

    return {it, XXH3_64bits_digest(&state)};
}

} // namespace NKikimr
