#pragma once

#include <util/system/types.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

namespace NKikimr {

class TCountMinSketch {
private:
    ui64 Width_;
    ui64 Depth_;
    ui64 ElementCount_;

private:
    static ui64 Hash(const char* data, size_t size, size_t hashIndex);

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
        return StaticSize(Width_, Depth_);
    }

    static size_t StaticSize(ui64 width, ui64 depth) {
        return sizeof(TCountMinSketch) + width * depth * sizeof(ui32);
    }

    size_t GetWidth() const {
        return Width_;
    }

    size_t GetDepth() const {
        return Depth_;
    }

    size_t GetElementCount() const {
        return ElementCount_;
    }

    TStringBuf AsStringBuf() const {
        return TStringBuf(reinterpret_cast<const char*>(this), GetSize());
    }

    void Count(const char* data, size_t size);

    ui32 Probe(const char* data, size_t size) const;

    TMaybe<ui32> GetOverlappingCardinality(const TCountMinSketch& rhs) const;

    TCountMinSketch& operator+=(const TCountMinSketch& rhs);
};

static_assert(sizeof(TCountMinSketch) == 24);

} // namespace NKikimr
