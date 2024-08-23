#pragma once

#include <util/system/types.h>
#include <util/generic/strbuf.h>

#include <vector>

namespace NKikimr {

class TCountMinSketch {
private:
    ui64 Version = 1;
    ui64 Width;
    ui64 Depth;
    ui64 ElementCount;

private:
    static ui64 Hash(const char* data, size_t size, size_t hashIndex);

    static size_t StaticSize(ui64 width, ui64 depth) {
        return sizeof(TCountMinSketch) + width * depth * sizeof(ui32);
    }

    const ui32* Buckets() const {
        return reinterpret_cast<const ui32*>(this + 1);
    }

    ui32* Buckets() {
        return reinterpret_cast<ui32*>(this + 1);
    }

public:
    static TCountMinSketch* Create(ui64 width = 256, ui64 depth = 8);
    static TCountMinSketch* FromString(const char* data, size_t size);

    void operator delete(void* data) noexcept;

    TCountMinSketch() = delete;
    TCountMinSketch(const TCountMinSketch&) = delete;

    size_t GetSize() const {
        return StaticSize(Width, Depth);
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

    TStringBuf AsStringBuf() const {
        return TStringBuf(reinterpret_cast<const char*>(this), GetSize());
    }

    void Count(const char* data, size_t size);

    ui32 Probe(const char* data, size_t size) const;

    TCountMinSketch& operator+=(const TCountMinSketch& rhs);
};

static_assert(sizeof(TCountMinSketch) == 32);

} // NKikimr
