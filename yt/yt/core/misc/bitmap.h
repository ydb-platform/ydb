#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <util/system/align.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NBitmapDetail {
    // No need to use word size other than ui8.
    // Performance is same for ui8, ui16, ui32 and ui64 and is equal to 200 Mb/s.

    // BM_Bitmap_ui8      153148 ns       153108 ns         4492 bytes_per_second=204.105M/s
    // BM_Bitmap_ui16     151758 ns       151720 ns         4462 bytes_per_second=205.971M/s
    // BM_Bitmap_ui32     150381 ns       150352 ns         4672 bytes_per_second=207.846M/s
    // BM_Bitmap_ui64     152476 ns       152442 ns         4668 bytes_per_second=204.996M/s

    // We do not want to force alignment when reading bitmaps, hence we should prefer ui8.
    // Alignment/padding in serialization should be used explicitly.

    using TByte = ui8;
    static constexpr size_t Bits = 8 * sizeof(TByte);
    constexpr size_t SerializationAlignment = 8;

    static constexpr TByte GetBitMask(size_t index)
    {
        return TByte(1) << (index % Bits);
    }

    static constexpr size_t GetWordIndex(size_t index)
    {
        return index / Bits;
    }

    static constexpr size_t GetByteSize(size_t size)
    {
        return (size + Bits - 1) / Bits;
    }
} // namespace NBitmapDetail

////////////////////////////////////////////////////////////////////////////////

class TMutableBitmap
{
public:
    using TByte = NBitmapDetail::TByte;

    explicit TMutableBitmap(void* ptr = nullptr)
        : Ptr_(static_cast<TByte*>(ptr))
    { }

    bool operator [] (size_t index) const
    {
        return Ptr_[NBitmapDetail::GetWordIndex(index)] & NBitmapDetail::GetBitMask(index);
    }

    void Set(size_t index)
    {
        Ptr_[NBitmapDetail::GetWordIndex(index)] |= NBitmapDetail::GetBitMask(index);
    }

    void Set(size_t index, bool value)
    {
        auto mask = NBitmapDetail::GetBitMask(index);
        auto& word = Ptr_[NBitmapDetail::GetWordIndex(index)];
        word = (word & ~mask) | (-value & mask);
    }

    TByte* GetData()
    {
        return Ptr_;
    }

    const TByte* GetData() const
    {
        return Ptr_;
    }

private:
    TByte* Ptr_;
};

class TBitmap
{
public:
    using TByte = NBitmapDetail::TByte;

    TBitmap(const TMutableBitmap& bitmap)
        : Ptr_(bitmap.GetData())
    { }

    explicit TBitmap(const void* ptr = nullptr)
        : Ptr_(static_cast<const TByte*>(ptr))
    { }

    bool operator [] (size_t index) const
    {
        return Ptr_[NBitmapDetail::GetWordIndex(index)] & NBitmapDetail::GetBitMask(index);
    }

    void Prefetch(size_t index) const
    {
        // Prefetch data into all levels of the cache hierarchy.
        Y_PREFETCH_READ(Ptr_ + NBitmapDetail::GetWordIndex(index), 3);
    }

    const TByte* GetData() const
    {
        return Ptr_;
    }

private:
    const TByte* Ptr_;
};

static_assert(sizeof(TBitmap) == sizeof(void*), "Do not modify TBitmap. Write your own class.");

////////////////////////////////////////////////////////////////////////////////

void CopyBitmap(void* dst, ui32 dstOffset, const void* src, ui32 srcOffset, ui32 count);

////////////////////////////////////////////////////////////////////////////////

// Like TBlobOutput.
class TBitmapOutput
{
public:
    using TByte = NBitmapDetail::TByte;

    explicit TBitmapOutput(size_t bitCapacity = 0)
    {
        if (bitCapacity) {
            Chunks_.reserve(
                AlignUp(NBitmapDetail::GetByteSize(bitCapacity), NBitmapDetail::SerializationAlignment));
        }
    }

    void Append(bool value)
    {
        if (Chunks_.size() * NBitmapDetail::Bits == BitSize_) {
            Chunks_.resize(Chunks_.size() + NBitmapDetail::SerializationAlignment, 0);
        }
        TMutableBitmap(Chunks_.data()).Set(BitSize_++, value);
    }

    bool operator[](size_t bitIndex) const
    {
        YT_ASSERT(bitIndex < BitSize_);
        return Chunks_[NBitmapDetail::GetWordIndex(bitIndex)] & NBitmapDetail::GetBitMask(bitIndex);
    }

    template <class TTag>
    TSharedRef Flush()
    {
        YT_ASSERT(Chunks_.size() == GetByteSize());
        return TSharedRef::MakeCopy<TTag>(TRef(GetData(), GetByteSize()));
    }

    const TByte* GetData() const
    {
        return Chunks_.data();
    }

    size_t GetBitSize() const
    {
        return BitSize_;
    }

    size_t GetByteSize() const
    {
        return AlignUp(Chunks_.size() * sizeof(TByte), NBitmapDetail::SerializationAlignment);
    }

private:
    TCompactVector<TByte, NBitmapDetail::SerializationAlignment> Chunks_;
    size_t BitSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReadOnlyBitmap
    : public TBitmap
{
public:
    using TByte = NBitmapDetail::TByte;

    TReadOnlyBitmap() = default;

    TReadOnlyBitmap(const void* chunks, size_t bitSize)
        : TBitmap(chunks)
        , BitSize_(bitSize)
    { }

    void Reset(const void* chunks, size_t bitSize)
    {
        YT_VERIFY(chunks);
        static_cast<TBitmap&>(*this) = TBitmap(chunks);
        BitSize_ = bitSize;
    }

    size_t GetByteSize() const
    {
        return NBitmapDetail::GetByteSize(BitSize_);
    }

    TRef GetData() const
    {
        return TRef(TBitmap::GetData(), GetByteSize());
    }

private:
    size_t BitSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
