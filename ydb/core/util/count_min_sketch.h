#pragma once

#include <util/system/types.h>

#include <vector>

namespace NKikimr {

class TCountMinSketch {
private:
    const size_t Width;
    const size_t Depth;

    size_t ElementCount{0};
    std::vector<ui32> Buckets;

private:
    static ui64 Hash(const char* data, size_t size, size_t hashIndex);

public:
    explicit TCountMinSketch(size_t width = 256, size_t depth = 8)
        : Width(width)
        , Depth(depth)
    {
        Buckets.resize(Width * Depth, 0);
    }

    size_t GetWidth() const {
        return Width;
    }

    size_t GetDepth() const {
        return Depth;
    }

    size_t GetElementCount() const {
        return ElementCount;
    }

    void Count(const char* data, size_t size);

    ui32 Probe(const char* data, size_t size) const;

    TCountMinSketch& operator+=(TCountMinSketch& rhs);
};

} // NKikimr
