#ifndef BIT_PACKED_UNSIGNED_VECTOR_INL_H_
#error "Direct inclusion of this file is not allowed, include bit_packed_unsigned_vector.h"
// For the sake of sane code completion.
#include "bit_packed_unsigned_vector.h"
#endif

#include "bit_packing.h"

#include <util/generic/bitops.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline ui64 GetWidth(ui64 value)
{
    return (value == 0) ? 0 : MostSignificantBit(value) + 1;
}

inline size_t CompressedUnsignedVectorSizeInWords(ui64 maxValue, size_t count)
{
    // One word for the header.
    return 1 + ((GetWidth(maxValue) * count + 63ULL) >> 6ULL);
}

inline size_t CompressedUnsignedVectorSizeInBytes(ui64 maxValue, size_t count)
{
    static size_t wordSize = sizeof(ui64);
    return CompressedUnsignedVectorSizeInWords(maxValue, count) * wordSize;
}

template <class T>
typename std::enable_if<std::is_unsigned<T>::value, size_t>::type
BitPackUnsignedVector(TRange<T> values, ui64 maxValue, ui64* dst)
{
    ui64 width = GetWidth(maxValue);
    ui64 header = values.Size();

    // Check that most significant byte is empty.
    YT_VERIFY((MaskLowerBits(8, 56) & header) == 0);
    header |= width << 56;

    // Save header.
    *dst = header;

    if (maxValue == 0) {
        // All values are zeros.
        return 1;
    }

    ui64* word = dst + 1;

    ui8 offset = 0;
    if (width < 64) {
        for (auto value : values) {
            // Cast to ui64 to do proper shifts.
            ui64 x = value;
            if (offset + width < 64) {
                *word |= (x << offset);
                offset += width;
            } else {
                *word |= (x << offset);
                offset = offset + width;
                offset &= 0x3F;
                ++word;
                x >>= width - offset;
                if (x > 0) {
                    // This is important not to overstep allocated boundaries.
                    *word |= x;
                }
            }
        }
    } else {
        // Custom path for 64-bits (especially useful since right shift on 64 bits does nothing).
        for (auto value : values) {
            *word = value;
            ++word;
        }
    }

    return (offset == 0 ? 0 : 1) + word - dst;
}

template <class T>
TSharedRef BitpackVector(TRange<T> values, ui64 maxValue, ui32* size, ui8* width)
{
    auto data = BitPackUnsignedVector(values, maxValue);
    *size = GetCompressedVectorSize(data.Begin());
    *width = GetCompressedVectorWidth(data.Begin());
    return data;
}

template <class T>
typename std::enable_if<std::is_unsigned<T>::value, TSharedRef>::type
BitPackUnsignedVector(TRange<T> values, ui64 maxValue)
{
    struct TCompressedUnsignedVectorTag {};

    size_t size = CompressedUnsignedVectorSizeInBytes(maxValue, values.Size());
    auto data = TSharedMutableRef::Allocate<TCompressedUnsignedVectorTag>(size);
    auto actualSize = BitPackUnsignedVector(values, maxValue, reinterpret_cast<ui64*>(data.Begin()));
    YT_VERIFY(size == actualSize * sizeof(ui64));

    return data;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, bool Scan>
TBitPackedUnsignedVectorReader<T, Scan>::TBitPackedUnsignedVectorReader(const ui64* data)
    : Data_(data + 1)
    , Size_(*data & MaskLowerBits(56))
    , Width_(*data >> 56)
{
    if (Scan) {
        UnpackValues();
    }
}

template <class T, bool Scan>
TBitPackedUnsignedVectorReader<T, Scan>::TBitPackedUnsignedVectorReader()
    : Data_(nullptr)
    , Size_(0)
    , Width_(0)
{ }

template <class T, bool Scan>
inline T TBitPackedUnsignedVectorReader<T, Scan>::operator[] (size_t index) const
{
    YT_ASSERT(index < Size_);
    if (Scan) {
        return Values_[index];
    } else {
        return GetValue(index);
    }
}

template <class T, bool Scan>
inline size_t TBitPackedUnsignedVectorReader<T, Scan>::GetSize() const
{
    return Size_;
}

template <class T, bool Scan>
inline size_t TBitPackedUnsignedVectorReader<T, Scan>::GetByteSize() const
{
    if (Data_) {
        return (1 + ((Width_ * Size_ + 63ULL) >> 6ULL)) * sizeof(ui64);
    } else {
        return 0;
    }
}

template <class T, bool Scan>
TRange<T> TBitPackedUnsignedVectorReader<T, Scan>::GetData() const
{
    return TRange(Values_, Values_ + Size_);
}

template <class T, bool Scan>
T TBitPackedUnsignedVectorReader<T, Scan>::GetValue(size_t index) const
{
    if (Width_ == 0) {
        return 0;
    }

    ui64 bitIndex = index * Width_;
    const ui64* word = Data_ + (bitIndex >> 6);
    ui8 offset = bitIndex & 0x3F;

    ui64 w1 = (*word) >> offset;
    if (offset + Width_ > 64) {
        ++word;
        ui64 w2 = (*word & MaskLowerBits((offset + Width_) & 0x3F)) << (64 - offset);
        return static_cast<T>(w1 | w2);
    } else {
        return static_cast<T>(w1 & MaskLowerBits(Width_));
    }
}

namespace {

template <class T, int Width, int Remaining>
struct TCompressedUnsignedVectorUnrolledReader
{
    static Y_FORCE_INLINE void Do(ui64& data, T*& output, ui64 mask)
    {
        *output++ = static_cast<T>(data & mask);
        data >>= Width;
        TCompressedUnsignedVectorUnrolledReader<T, Width, Remaining - 1>::Do(data, output, mask);
    }
};

template <class T, int Width>
struct TCompressedUnsignedVectorUnrolledReader<T, Width, 0>
{
    static Y_FORCE_INLINE void Do(ui64& /*data*/, T*& /*output*/, ui64 /*mask*/)
    { }
};

} // namespace

template <class T, bool Scan>
template <int Width>
void TBitPackedUnsignedVectorReader<T, Scan>::UnpackValuesUnrolled()
{
    constexpr bool Aligned = (Width % 64 == 0);
    constexpr int UnrollFactor = (64 / Width) - (Aligned ? 0 : 1);

    const ui64* input = Data_;
    auto* output = ValuesHolder_.get();
    auto* outputEnd = output + Size_;
    ui8 offset = 0;
    ui64 mask = MaskLowerBits(Width_);

    ui64 data = *input++;
    while (output < outputEnd) {
        TCompressedUnsignedVectorUnrolledReader<T, Width, UnrollFactor>::Do(data, output, mask);
        offset += UnrollFactor * Width;
        if (!Aligned && offset + Width <= 64) {
            TCompressedUnsignedVectorUnrolledReader<T, Width, 1>::Do(data, output, mask);
            offset += Width;
        }
        if (output >= outputEnd) {
            break;
        }
        if (offset == 64) {
            offset = 0;
            data = *input++;
        } else {
            ui64 nextData = *input++;
            ui8 nextOffset = (offset + Width) & 0x3F;
            *output++ = static_cast<T>(((nextData & MaskLowerBits(nextOffset)) << (64 - offset)) | data);
            data = nextData;
            offset = nextOffset;
            data >>= offset;
        }
    }
}

template <class T, bool Scan>
void TBitPackedUnsignedVectorReader<T, Scan>::UnpackValuesFallback()
{
    const ui64* input = Data_;
    auto* output = ValuesHolder_.get();
    auto* outputEnd = output + Size_;
    ui8 offset = 0;
    ui64 mask = MaskLowerBits(Width_);
    while (output != outputEnd) {
        ui64 w1 = *input >> offset;
        if (offset + Width_ > 64) {
            ++input;
            ui64 w2 = (*input & MaskLowerBits((offset + Width_) & 0x3F)) << (64 - offset);
            *output = static_cast<T>(w1 | w2);
        } else {
            *output = static_cast<T>(w1 & mask);
        }

        offset = (offset + Width_) & 0x3F;
        if (offset == 0) {
            ++input;
        }

        ++output;
    }
}

template <class T, bool Scan>
template <class S>
void TBitPackedUnsignedVectorReader<T, Scan>::UnpackValuesAligned()
{
    const auto* input = reinterpret_cast<const S*>(Data_);
    auto* output = ValuesHolder_.get();
    auto* outputEnd = output + Size_;
    while (output != outputEnd) {
        *output++ = *input++;
    }
}

template <class T, bool Scan>
void TBitPackedUnsignedVectorReader<T, Scan>::UnpackValues()
{
    if (Size_ == 0) {
        return;
    }

    // Zero-copy path.
    if (Width_ ==  8 && sizeof(T) == 1 ||
        Width_ == 16 && sizeof(T) == 2 ||
        Width_ == 32 && sizeof(T) == 4 ||
        Width_ == 64 && sizeof(T) == 8)
    {
        Values_ = reinterpret_cast<const T*>(Data_);
        return;
    }

    // NB: Unrolled loop may unpack more values than actually needed.
    // Make sure we have enough room for them.
    auto valuesSize = Size_ + ((Width_ > 0) ? (64 / Width_ + 1) : 0);
    ValuesHolder_.reset(new T[valuesSize]);
    Values_ = ValuesHolder_.get();

    switch (Width_) {
        case  0: std::fill(ValuesHolder_.get(), ValuesHolder_.get() + Size_, 0); break;
        #define UNROLLED(width)      case width: UnpackValuesUnrolled<width>(); break;
        #define ALIGNED(width, type) case width: UnpackValuesAligned<type>(); break;
        UNROLLED( 1)
        UNROLLED( 2)
        UNROLLED( 3)
        UNROLLED( 4)
        UNROLLED( 5)
        UNROLLED( 6)
        UNROLLED( 7)
        ALIGNED ( 8, ui8)
        UNROLLED( 9)
        UNROLLED(10)
        UNROLLED(11)
        UNROLLED(12)
        UNROLLED(13)
        UNROLLED(14)
        UNROLLED(15)
        ALIGNED (16, ui16)
        UNROLLED(17)
        UNROLLED(18)
        UNROLLED(19)
        UNROLLED(20)
        UNROLLED(21)
        UNROLLED(22)
        UNROLLED(23)
        UNROLLED(24)
        UNROLLED(25)
        UNROLLED(26)
        UNROLLED(27)
        UNROLLED(28)
        UNROLLED(29)
        UNROLLED(30)
        UNROLLED(31)
        ALIGNED (32, ui32)
        // NB: ALIGNED(64, ui64) is redundant, cf. zero-copy path above.
        #undef UNROLLED
        #undef ALIGNED
        default: UnpackValuesFallback(); break;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
