#include "stack_count_min_sketch.h"

#include <util/system/compiler.h>

namespace NKikimr {

template <ui32 W, ui32 D>
TStackAllocatedCountMinSketch<W, D> TStackAllocatedCountMinSketch<W, D>::FromString(const char* data, size_t size) {
    Y_ABORT_UNLESS(GetSize() == size);

    auto* from = reinterpret_cast<const TStackAllocatedCountMinSketch<W, D>*>(data);
    return TStackAllocatedCountMinSketch<W, D>(*from);
}

template <ui32 W, ui32 D>
ui64 TStackAllocatedCountMinSketch<W, D>::Hash(const char* data, size_t size, size_t hashIndex) {
    // fnv1a
    ui64 hash = 14695981039346656037ULL + 31 * hashIndex;
    const unsigned char* ptr = (const unsigned char*)data;
    for (size_t i = 0; i < size; ++i, ++ptr) {
        hash = hash ^ (*ptr);
        hash = hash * 1099511628211ULL;
    }
    return hash;
}

template <ui32 W, ui32 D>
void TStackAllocatedCountMinSketch<W, D>::Count(const char* data, size_t size) {
    ui32* start = Buckets();
    for (size_t d = 0; d < D; ++d, start += W) {
        ui64 hash = Hash(data, size, d);
        ui32* bucket = start + hash % W;
        if (Y_LIKELY(*bucket < std::numeric_limits<ui32>::max())) {
            ++*bucket;
        }
    }
    ++ElementCount;
}

template <ui32 W, ui32 D>
ui32 TStackAllocatedCountMinSketch<W, D>::Probe(const char* data, size_t size) const {
    ui32 minValue = std::numeric_limits<ui32>::max();
    const ui32* start = Buckets();
    for (size_t d = 0; d < D; ++d, start += W) {
        ui64 hash = Hash(data, size, d);
        const ui32* bucket = start + hash % W;
        minValue = std::min(minValue, *bucket);
    }
    return minValue;
}

template <ui32 W, ui32 D>
TStackAllocatedCountMinSketch<W, D>& TStackAllocatedCountMinSketch<W, D>::operator+=(const TStackAllocatedCountMinSketch<W, D>& rhs) {
    ui32* dst = Buckets();
    const ui32* src = rhs.Buckets();
    ui32* end = dst + W * D;
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

// explicit instantation
template class TStackAllocatedCountMinSketch<256, 8>;

} // namespace NKikimr
