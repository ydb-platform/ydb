#pragma once

#include <util/generic/buffer.h>
#include <util/generic/yexception.h>
#include <util/memory/blob.h>
#include <util/string/cast.h>
#include <util/system/src_root.h>

#include <vector>

#include <bitset>

namespace NErasure {

//! The maximum total number of blocks our erasure codec can handle.
static constexpr int MaxTotalPartCount = 16;

//! Max word size to use. `w` in jerasure notation
static constexpr int MaxWordSize = 8;

//! Max threshold to generate bitmask for CanRepair indices for LRC encoding.
static constexpr int BitmaskOptimizationThreshold = 22;

//! A vector type for holding block indexes.
using TPartIndexList = std::vector<int>;

//! Each bit corresponds to a possible block index.
using TPartIndexSet = std::bitset<MaxTotalPartCount>;

template <class TBlobType>
struct ICodec;

struct TDefaultCodecTraits {
    using TBlobType = TBlob;
    using TMutableBlobType = TBlob;
    using TBufferType = TBuffer;

    static inline TMutableBlobType AllocateBlob(size_t size) {
        TBufferType buffer(size);
        buffer.Resize(size);
        // The buffer is cleared after this call so no use after free.
        return TBlob::FromBuffer(buffer);
    }

    // AllocateBuffer must fill the memory with 0.
    static inline TBufferType AllocateBuffer(size_t size) {
        TBufferType buffer(size);
        buffer.Fill(0, size);
        return buffer;
    }

    static inline TBlobType FromBufferToBlob(TBufferType&& blob) {
        return TBlobType::FromBuffer(blob);
    }
};

} // namespace NErasure
