#pragma once

#include <util/system/types.h>
#include <util/generic/strbuf.h>

namespace NKikimr {

template <ui32 Width = 256, ui32 Depth = 8>
class TStackAllocatedCountMinSketch {
private:
    ui64 ElementCount = 0;
    ui32 Data[Width * Depth] = {0};

private:
    static ui64 Hash(const char* data, size_t size, size_t hashIndex);

    const ui32* Buckets() const {
        return Data;
    }

    ui32* Buckets() {
        return Data;
    }

public:
    static TStackAllocatedCountMinSketch FromString(const char* data, size_t size);

    TStackAllocatedCountMinSketch() = default;

    static size_t GetSize() {
        return sizeof(TStackAllocatedCountMinSketch<Width, Depth>);
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

    TStackAllocatedCountMinSketch& operator+=(const TStackAllocatedCountMinSketch& rhs);
};

static_assert(sizeof(TStackAllocatedCountMinSketch<256, 8>) == 256 * 8 * sizeof(ui32) + sizeof(ui64));

} // NKikimr
