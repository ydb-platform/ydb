#include "erasure.h"

#include <ydb/core/base/logoblob.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

#define XXH_INLINE_ALL
#include <contrib/libs/xxhash/xxhash.h>

namespace NKikimr {

namespace {

template<typename TIterator>
std::pair<TIterator, ui64> CalculateXxh3Hash(const TLogoBlobID& id, TErasureType erasure, TIterator it) {
    XXH3_state_t state;
    if (XXH3_64bits_reset(&state) != XXH_OK) {
        Y_FAIL("CalculateXxh3Hash: XXH3_64bits_reset failed");
    }

    size_t numBytes = erasure.PartUserSize(id.BlobSize());
    while (numBytes && it.Valid()) {
        const size_t n = Min(numBytes, it.ContiguousSize());
        if (XXH3_64bits_update(&state, it.ContiguousData(), n) != XXH_OK) {
            Y_FAIL("CalculateXxh3Hash: XXH3_64bits_update failed on content processing");
        }
        numBytes -= n;
        it += n;
    }

    return {it, XXH3_64bits_digest(&state)};
}

} // namespace


bool TChecksum::IsRequired(const TLogoBlobID& id) noexcept {
    switch (static_cast<TErasureType::ECrcMode>(id.CrcMode())) {
        case TErasureType::CrcModeNone:
            return false;
        case TErasureType::CrcModeWholePart:
            return true;
        default:
            Y_FAIL("Unknown crcMode: %d", id.CrcMode());
    }
}

bool TChecksum::IsValid(const TLogoBlobID& id, TErasureType erasure, const TRope& rope) {
    if (!IsRequired(id)) {
        return true;
    }
    if (rope.IsEmpty()) {
        return erasure.PartSize(id) == 0;
    }
    if (rope.size() != erasure.PartSize(static_cast<TErasureType::ECrcMode>(id.CrcMode()), id.BlobSize())) {
        return false;
    }

    auto [it, hash] = CalculateXxh3Hash(id, erasure, rope.Begin());
    ui64 expectedChecksum;
    it.ExtractPlainDataAndAdvance(&expectedChecksum, sizeof(expectedChecksum));
    return hash == expectedChecksum;
}

TRope TChecksum::Append(const TLogoBlobID& id, TErasureType erasure, TRope rope) {
    if (!IsRequired(id)) {
        return rope;
    }
    const auto partSize = erasure.PartSize(
        static_cast<TErasureType::ECrcMode>(id.CrcMode()), id.BlobSize());
    if (rope.IsEmpty() && partSize == 0) {
        return rope;
    }

    const auto partUserSize = erasure.PartUserSize(id.BlobSize());
    Y_ABORT_UNLESS(rope.size() >= partUserSize,
                    "Error in TChecksum::Append:"
                    "blob part size# %" PRIu64 "is less than expected# %" PRIu64,
                    (ui64)rope.size(), partUserSize);

    auto [it, hash] = CalculateXxh3Hash(id, erasure, rope.Begin());
    const char* hashBuffer = reinterpret_cast<const char*>(&hash);
    if (rope.size() == partSize) {
        TRopeUtils::Memcpy(it, hashBuffer, sizeof(hash));
    } else if (rope.size() == partUserSize) {
        rope.Insert(rope.End(), TRcBuf::Copy(hashBuffer, sizeof(hash)));
    } else {
        Y_FAIL("buffer is of unsupported size");
    }
    return rope;
}

TRope TChecksum::Remove(const TLogoBlobID& id, TErasureType erasure, TRope rope) {
    if (!IsRequired(id)) {
        return rope;
    }
    if (rope.IsEmpty() || rope.size() == erasure.PartUserSize(id.BlobSize())) {
        return rope;
    }
    const ui64 partSize = erasure.PartSize(
        static_cast<TErasureType::ECrcMode>(id.CrcMode()), id.BlobSize());
    Y_ABORT_UNLESS(
        rope.size() == partSize,
        "Error in TChecksum::Remove:"
        "blob part size# %" PRIu64 "is less than expected# %" PRIu64,
        (ui64)rope.size(), partSize);

    const ui64 partUserSize = erasure.PartUserSize(id.BlobSize());
    rope.Erase(rope.Position(partUserSize), rope.End());
    return rope;
}

} // namespace NKikimr
