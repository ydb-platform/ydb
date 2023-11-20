#ifndef BIT_PACKING_INL_H_
#error "Direct inclusion of this file is not allowed, include bit_packing.h"
// For the sake of sane code completion.
#include "bit_packing.h"
#endif
#undef BIT_PACKING_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline size_t GetCompressedVectorSize(const void* ptr)
{
    return *static_cast<const ui64*>(ptr) & MaskLowerBits(56);
}

inline size_t GetCompressedVectorWidth(const void* ptr)
{
    return *static_cast<const ui64*>(ptr) >> 56;
}

////////////////////////////////////////////////////////////////////////////////

inline TCompressedViewBase::TCompressedViewBase(ui32 size, ui8 width)
    : Size_(size)
    , Width_(width)
{ }

inline size_t TCompressedViewBase::GetSize() const
{
    return Size_;
}

inline size_t TCompressedViewBase::GetWidth() const
{
    return Width_;
}

inline size_t TCompressedViewBase::GetSizeInWords() const
{
    return 1 + (GetWidth() * GetSize() + WordSize - 1) / WordSize;
}

inline size_t TCompressedViewBase::GetSizeInBytes() const
{
    return GetSizeInWords() * sizeof(ui64);
}

////////////////////////////////////////////////////////////////////////////////

inline TCompressedVectorView::TCompressedVectorView(const ui64* ptr, ui32 size, ui8 width)
    // Considering width here helps to avoid extra branch when extracting element.
    : TCompressedViewBase(size, width)
    , Ptr_(ptr + (width != 0))
{
    YT_ASSERT(GetCompressedVectorSize(ptr) == size);
    YT_ASSERT(GetCompressedVectorWidth(ptr) == width);
}

inline TCompressedVectorView::TCompressedVectorView(const ui64* ptr)
    : TCompressedVectorView(ptr, GetCompressedVectorSize(ptr), GetCompressedVectorWidth(ptr))
{ }

inline void TCompressedVectorView::Prefetch(size_t index) const
{
    YT_ASSERT(index < GetSize());
    auto width = GetWidth();
    auto bitIndex = index * width;
    const auto* data = Ptr_ + bitIndex / WordSize;
    // Prefetch data into all levels of the cache hierarchy.
    Y_PREFETCH_READ(data, 3);
}

inline TCompressedVectorView::TWord TCompressedVectorView::operator[] (size_t index) const
{
    YT_ASSERT(index < GetSize());
    auto width = GetWidth();
    auto bitIndex = index * width;

    const auto* data = Ptr_ + bitIndex / WordSize;
    ui8 offset = bitIndex % WordSize;

    TWord w = data[0] >> offset;
    if (offset + width > WordSize) {
        w |= data[1] << (WordSize - offset);
    }

    return w & MaskLowerBits(width);
}

////////////////////////////////////////////////////////////////////////////////

inline TCompressedVectorView32::TCompressedVectorView32(const ui64* ptr, ui32 size, ui8 width)
    // Considering width here helps to avoid extra branch when extracting element.
    : TCompressedViewBase(size, width)
    , Ptr_(ptr + 1)
{
    YT_ASSERT(GetCompressedVectorSize(ptr) == size);
    YT_ASSERT(GetCompressedVectorWidth(ptr) == width);
}

inline TCompressedVectorView32::TCompressedVectorView32(const ui64* ptr)
    : TCompressedVectorView32(ptr, GetCompressedVectorSize(ptr), GetCompressedVectorWidth(ptr))
{ }

void TCompressedVectorView32::Prefetch(size_t index) const
{
    YT_ASSERT(index < GetSize());
    auto width = GetWidth();
    auto bitIndexEnd = (index + 1) * width;
    const auto* data = reinterpret_cast<const ui8*>(Ptr_) + (bitIndexEnd + 7) / 8;
    // Prefetch data into all levels of the cache hierarchy.
    Y_PREFETCH_READ(reinterpret_cast<const TWord*>(data) - 1, 3);
}

ui32 TCompressedVectorView32::operator[] (size_t index) const
{
    YT_ASSERT(index < GetSize());
    auto width = GetWidth();
    // Read without crossing upper memory bound of compressed view.
    auto bitIndexEnd = (index + 1) * width;
    auto byteIndexEnd = (bitIndexEnd + 7) / 8;

    const auto* data = reinterpret_cast<const ui8*>(Ptr_) + byteIndexEnd;
    // Unaligned read is used to eliminate extra branch.
    size_t offset = WordSize - width + bitIndexEnd - 8 * byteIndexEnd;
    TWord w = reinterpret_cast<const TWord*>(data)[-1] >> offset;
    return w & MaskLowerBits(width);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
