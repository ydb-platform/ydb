#include "count_min_sketch.h"

#include <util/system/compiler.h>

#include <cstdlib>

namespace NKikimr {

TCountMinSketch* TCountMinSketch::Create(ui64 width, ui64 depth) {
    auto size = StaticSize(width, depth);
    auto* data = ::malloc(size);
    auto* sketch = reinterpret_cast<TCountMinSketch*>(data);
    std::memset(sketch, 0, size);
    sketch->Width_ = width;
    sketch->Depth_ = depth;
    sketch->ElementCount_ = 0;
    return sketch;
}

TCountMinSketch* TCountMinSketch::FromString(const char* data, size_t size) {
    auto* from = reinterpret_cast<const TCountMinSketch*>(data);
    Y_ABORT_UNLESS(StaticSize(from->Width_, from->Depth_) == size);
    auto* dataDst = ::malloc(size);
    std::memcpy(dataDst, data, size);
    return reinterpret_cast<TCountMinSketch*>(dataDst);
}

void TCountMinSketch::operator delete(void* data) noexcept {
    ::free(data);
}

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
    ui32* start = Buckets();
    for (size_t d = 0; d < Depth_; ++d, start += Width_) {
        ui64 hash = Hash(data, size, d);
        ui32* bucket = start + hash % Width_;
        if (Y_LIKELY(*bucket < std::numeric_limits<ui32>::max())) {
            ++*bucket;
        }
    }
    ++ElementCount_;
}

ui32 TCountMinSketch::Probe(const char* data, size_t size) const {
    ui32 minValue = std::numeric_limits<ui32>::max();
    const ui32* start = Buckets();
    for (size_t d = 0; d < Depth_; ++d, start += Width_) {
        ui64 hash = Hash(data, size, d);
        const ui32* bucket = start + hash % Width_;
        minValue = std::min(minValue, *bucket);
    }
    return minValue;
}

TCountMinSketch& TCountMinSketch::operator+=(const TCountMinSketch& rhs) {
    if (Width_ != rhs.Width_ || Depth_ != rhs.Depth_) {
        return *this;
    }
    ui32* dst = Buckets();
    const ui32* src = rhs.Buckets();
    ui32* end = dst + Width_ * Depth_;
    for (; dst != end; ++dst, ++src) {
        ui32 sum = *dst + *src;
        if (Y_UNLIKELY(sum < *dst)) {
            *dst = std::numeric_limits<ui32>::max();
        } else {
            *dst = sum;
        }
    }
    ElementCount_ += rhs.ElementCount_;
    return *this;
}

} // namespace NKikimr
