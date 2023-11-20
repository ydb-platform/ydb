#include "count_min_sketch.h"

#include <util/system/compiler.h>

namespace NKikimr {

ui64 TCountMinSketch::Hash(const char* data, size_t size, size_t hashIndex) {
    // fnv1a
    ui64 hash = 14695981039346656037ULL + 31 * hashIndex;
    const unsigned char* ptr = (const unsigned char*)data;
    for (size_t i = 0; i < size; ++i, ++ptr) {
        hash = hash ^ (*ptr);
        hash = hash * 1099511628211ULL;
    }
    return hash;
}

void TCountMinSketch::Count(const char* data, size_t size) {
    ui32* start = Buckets.data();
    for (size_t d = 0; d < Depth; ++d, start += Width) {
        ui64 hash = Hash(data, size, d);
        ui32* bucket = start + hash % Width;
        if (Y_LIKELY(*bucket < std::numeric_limits<ui32>::max())) {
            ++*bucket;
        }
    }
    ++ElementCount;
}

ui32 TCountMinSketch::Probe(const char* data, size_t size) const {
    ui32 minValue = std::numeric_limits<ui32>::max();
    const ui32* start = Buckets.data();
    for (size_t d = 0; d < Depth; ++d, start += Width) {
        ui64 hash = Hash(data, size, d);
        const ui32* bucket = start + hash % Width;
        minValue = std::min(minValue, *bucket);
    }
    return minValue;
}

TCountMinSketch& TCountMinSketch::operator+=(TCountMinSketch& rhs) {
    if (Width != rhs.Width || Depth != rhs.Depth) {
        return *this;
    }
    ui32* dst = Buckets.data();
    ui32* src = rhs.Buckets.data();
    ui32* end = dst + Width * Depth;
    for (; dst != end; ++dst, ++src) {
        ui32 sum = *dst + *src;
        if (Y_UNLIKELY(sum < *dst)) {
            *dst = std::numeric_limits<ui32>::max();
        } else {
            *dst = sum;
        }
    }
    ElementCount += rhs.ElementCount;
    return *this;
}

} // namespace NKikimr
