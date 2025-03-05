#include "bitmap.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void CopyBitmap(void* dst, ui32 dstOffset, const void* src, ui32 srcOffset, ui32 count)
{
    if (!count) {
        return;
    }

    auto dstOffsetSave = dstOffset;
    auto srcOffsetSave = srcOffset;
    Y_UNUSED(srcOffsetSave);
    Y_UNUSED(dstOffsetSave);

    using TWord = ui64;

    constexpr ui8 Bits = 8 * sizeof(TWord);

    auto* dstPtr = static_cast<TWord*>(dst);
    auto* srcPtr = static_cast<const TWord*>(src);

    dstPtr += dstOffset / Bits;
    dstOffset %= Bits;

    srcPtr += srcOffset / Bits;
    srcOffset %= Bits;

    auto* srcEnd = srcPtr + (srcOffset + count + Bits - 1) / Bits;
    auto* dstEnd = dstPtr + (dstOffset + count + Bits - 1) / Bits;

    {
        // Read min(count, Bits - dstOffset).
        auto word = srcPtr[0] >> srcOffset;
        if (srcOffset + count > Bits && srcOffset > dstOffset) {
            word |= srcPtr[1] << (Bits - srcOffset);
        }

        TWord firstWordMask = (TWord(1) << dstOffset) - 1;
        *dstPtr++ = (*dstPtr & firstWordMask) | (word << dstOffset);

        srcOffset += Bits - dstOffset;
        // Now dstOffset is zero.

        srcPtr += srcOffset / Bits;
        srcOffset %= Bits;
    }

    if (srcOffset) {
        if (srcPtr != srcEnd) {
            auto srcWord = *srcPtr++;
            auto dstWord = srcWord >> srcOffset;
            while (srcPtr != srcEnd) {
                srcWord = *srcPtr++;
                dstWord |= srcWord << (Bits - srcOffset);
                *dstPtr++ = dstWord;
                dstWord = srcWord >> srcOffset;
            }

            if (dstPtr != dstEnd) {
                *dstPtr++ = dstWord;
            }
        }
    } else {
        while (srcPtr != srcEnd) {
            *dstPtr++ = *srcPtr++;
        }
    }

#ifndef NDEBUG
    TBitmap srcBitmap(src);
    TBitmap dstBitmap(dst);

    for (int index = 0; index < static_cast<int>(count); ++index) {
        YT_VERIFY(srcBitmap[srcOffsetSave + index] == dstBitmap[dstOffsetSave + index]);
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

TCompactBitmap::TCompactBitmap()
{
    SetUninitializedState();
}

TCompactBitmap::~TCompactBitmap()
{
    if (HoldsAllocation()) {
        delete[] Ptr_;
    }
}

TCompactBitmap::TCompactBitmap(TCompactBitmap&& other)
{
    Ptr_ = other.Ptr_;
    other.SetUninitializedState();
}

TCompactBitmap& TCompactBitmap::operator=(TCompactBitmap&& other)
{
    if (this == &other) {
        return *this;
    }

    if (HoldsAllocation()) {
        delete[] Ptr_;
    }

    Ptr_ = other.Ptr_;
    other.SetUninitializedState();

    return *this;
}

void TCompactBitmap::Initialize(int bitSize)
{
    if (!IsCompact() && IsInitialized()) {
        delete[] Ptr_;
    }

    if (bitSize <= MaxInlineSize) {
        DataAsUi64() = CompactInitializedSentinel;
    } else {
        Ptr_ = new TByte[NBitmapDetail::GetByteSize(bitSize)];
        std::memset(Ptr_, 0, NBitmapDetail::GetByteSize(bitSize));
    }

    YT_VERIFY(IsInitialized());
}

void TCompactBitmap::CopyFrom(const TCompactBitmap& other, int bitSize)
{
    if (this == &other) {
        return;
    }

    if (HoldsAllocation()) {
        delete[] Ptr_;
    }

    if (!other.HoldsAllocation()) {
        Ptr_ = other.Ptr_;
        return;
    }

    Ptr_ = new TByte[NBitmapDetail::GetByteSize(bitSize)];
    std::memcpy(Ptr_, other.Ptr_, NBitmapDetail::GetByteSize(bitSize));
}


bool TCompactBitmap::operator[] (size_t index) const
{
    if (!IsInitialized()) {
        return false;
    }

    if (IsCompact()) {
        YT_VERIFY(index < MaxInlineSize);
        return (DataAsUi64() >> (index + 2)) & 1ull;
    } else {
        return TBitmap(Ptr_)[index];
    }
}

void TCompactBitmap::Set(size_t index)
{
    YT_VERIFY(IsInitialized());

    if (IsCompact()) {
        YT_VERIFY(index < MaxInlineSize);
        DataAsUi64() |= 1ull << (index + 2);
    } else {
        TMutableBitmap(Ptr_).Set(index);
    }
}

ui64& TCompactBitmap::DataAsUi64()
{
    return reinterpret_cast<ui64&>(Ptr_);
}

ui64 TCompactBitmap::DataAsUi64() const
{
    return reinterpret_cast<ui64>(Ptr_);
}

bool TCompactBitmap::IsCompact() const
{
    return (DataAsUi64() & 0b11ull) == CompactInitializedSentinel;
}

bool TCompactBitmap::IsInitialized() const
{
    return (DataAsUi64() & 0b10ull) == 0;
}

bool TCompactBitmap::HoldsAllocation() const
{
    return IsInitialized() && !IsCompact();
}

void TCompactBitmap::SetUninitializedState()
{
    Ptr_ = reinterpret_cast<TByte*>(UninitializedSentinel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
