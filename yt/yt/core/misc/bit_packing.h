#pragma once

#include <yt/yt/core/misc/public.h>

#include <numeric>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCompressedViewBase
{
public:
    using TWord = ui64;
    static constexpr ui8 WordSize = sizeof(TWord) * 8;

    TCompressedViewBase() = default;

    TCompressedViewBase(ui32 size, ui8 width);

    size_t GetSize() const;
    size_t GetWidth() const;
    size_t GetSizeInWords() const;
    size_t GetSizeInBytes() const;

private:
    ui32 Size_ = 0;
    ui8 Width_ = 0;
};

class TCompressedVectorView
    : public TCompressedViewBase
{
public:
    TCompressedVectorView() = default;

    TCompressedVectorView(const ui64* ptr, ui32 size, ui8 width);

    explicit TCompressedVectorView(const ui64* ptr);

    Y_FORCE_INLINE void Prefetch(size_t index) const;

    Y_FORCE_INLINE TWord operator[] (size_t index) const;

    template <class T>
    void UnpackTo(T* output);

    template <class T>
    void UnpackTo(T* output, ui32 start, ui32 end);

protected:
    static constexpr int WidthBitsOffset = 56;

    const ui64* Ptr_ = nullptr;
};

class TCompressedVectorView32
    : public TCompressedViewBase
{
public:
    TCompressedVectorView32() = default;

    TCompressedVectorView32(const ui64* ptr, ui32 size, ui8 width);

    explicit TCompressedVectorView32(const ui64* ptr);

    Y_FORCE_INLINE void Prefetch(size_t index) const;

    Y_FORCE_INLINE ui32 operator[] (size_t index) const;

private:
    const ui64* Ptr_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

size_t GetCompressedVectorSize(const void* ptr);
size_t GetCompressedVectorWidth(const void* ptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define BIT_PACKING_INL_H_
#include "bit_packing-inl.h"
#undef BIT_PACKING_INL_H_
