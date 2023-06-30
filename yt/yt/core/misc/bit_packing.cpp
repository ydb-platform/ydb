#include "bit_packing.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TInput, class TOutput>
void DoUnpackSimple(const TInput* input, TOutput* output, TOutput* outputEnd, ui8 width, ui8 bitOffset)
{
    constexpr ui8 Bits = sizeof(TInput) * 8;
    YT_VERIFY(width != 0);

    TInput mask = MaskLowerBits(width);
    while (output != outputEnd) {
        TInput word = input[0] >> bitOffset;

        if (bitOffset + width > Bits) {
            word |= input[1] << (Bits - bitOffset);
        }

        *output++ = static_cast<TOutput>(word & mask);

        bitOffset += width;
        input += bitOffset / Bits;
        bitOffset &= Bits - 1;
    }
}

template <class TOutput, int Width, int Remaining>
struct TBitPackedUnroller
{
    static constexpr ui64 Mask = (1ULL << Width) - 1;
    static constexpr ui8 UnrollFactor = 64 / Width;
    static constexpr ui8 Index = UnrollFactor - Remaining;

    static Y_FORCE_INLINE void Do(ui64 data, TOutput* output)
    {
        output[Index] = static_cast<TOutput>((data >> (Width * Index)) & Mask);
        TBitPackedUnroller<TOutput, Width, Remaining - 1>::Do(data, output);
    }
};

template <class TOutput, int Width>
struct TBitPackedUnroller<TOutput, Width, 0>
{
    static Y_FORCE_INLINE void Do(ui64 /*data*/, TOutput* /*output*/)
    { }
};

template <int Width, class TInput, class TOutput>
void DoUnpackUnrolled(const TInput* input, TOutput* output, TOutput* outputEnd, ui8 bitOffset)
{
    constexpr ui8 Bits = sizeof(TInput) * 8;
    constexpr TInput Mask = (1ULL << Width) - 1;

    constexpr ui8 UnrollFactor = Bits / Width;
    constexpr ui8 UnrollWidth = UnrollFactor * Width;

    while (output + UnrollFactor < outputEnd) {
        TInput word = input[0] >> bitOffset;

        if (bitOffset + UnrollWidth > Bits) {
            word |= input[1] << (Bits - bitOffset);
        }

        bitOffset += UnrollWidth;
        input += bitOffset / Bits;
        bitOffset &= Bits - 1;

        for (int i = 0; i < static_cast<int>(UnrollFactor); ++i) {
            *output++ = static_cast<TOutput>(word & Mask);
            word >>= Width;
        }
    }

    DoUnpackSimple(input, output, outputEnd, Width, bitOffset);
}

template <int Width, class TInput, class TOutput>
Y_NO_INLINE void UnpackValues(const TInput* input, TOutput* output, TOutput* outputEnd, size_t startOffset = 0)
{
    constexpr ui8 Bits = sizeof(TInput) * 8;
    static_assert(Width != 0);

    ui8 bitOffset = startOffset * Width % Bits;
    input += startOffset * Width / Bits;

    DoUnpackUnrolled<Width>(input, output, outputEnd, bitOffset);
}

template <class TInput, class TOutput>
Y_NO_INLINE void UnpackValuesFallback(
    const TInput* input,
    TOutput* output,
    TOutput* outputEnd,
    ui8 width,
    size_t startOffset = 0)
{
    constexpr ui8 Bits = sizeof(TInput) * 8;
    YT_VERIFY(width != 0);

    ui8 bitOffset = startOffset * width % Bits;
    input += startOffset * width / Bits;

    DoUnpackSimple(input, output, outputEnd, width, bitOffset);
}

template <class TInput, class TOutput, ui8 Width, ui8 Remaining>
struct TUnroller
{
    static constexpr ui8 Bits = sizeof(TInput) * 8;
    static constexpr ui8 UnrollFactor = Bits / std::gcd(Width, Bits);
    static constexpr TInput Mask = (1ULL << Width) - 1;

    static Y_FORCE_INLINE void Do(const TInput* input, TOutput* output)
    {
        constexpr ui8 Index = UnrollFactor - Remaining;
        constexpr ui8 Offset = Index * Width % Bits;

        auto value = input[Index * Width / Bits] >> Offset;
        if constexpr (Offset + Width > Bits) {
            value |= input[(Index + 1) * Width / Bits] << (Bits - Offset);
        }

        output[Index] = static_cast<TOutput>(value & Mask);

        TUnroller<TInput, TOutput, Width, Remaining - 1>::Do(input, output);
    }
};

template <class TInput, class TOutput, ui8 Width>
struct TUnroller<TInput, TOutput, Width, 0>
{
    static Y_FORCE_INLINE void Do(const TInput* /*input*/, TOutput* /*output*/)
    { }
};

template <int Width, class TInput, class TOutput>
Y_NO_INLINE void UnpackFullyUnrolled(const TInput* input, TOutput* output, TOutput* outputEnd)
{
    constexpr ui8 Bits = sizeof(TInput) * 8;
    constexpr ui8 Gcd = std::gcd(Width, Bits);
    constexpr ui8 UnrollFactor = Bits / Gcd;
    constexpr ui8 UnrollBatch = Width / Gcd;

    while (output + UnrollFactor < outputEnd) {
        TUnroller<TInput, TOutput, Width, UnrollFactor>::Do(input, output);
        output += UnrollFactor;
        input += UnrollBatch;
    }

    DoUnpackSimple(input, output, outputEnd, Width, 0);
}

template <class TInput, class TOutput>
void UnpackAligned(const TInput* input, TOutput* output, TOutput* outputEnd)
{
    while (output != outputEnd) {
        *output++ = *input++;
    }
}

} // namespace NDetail

template <class TOutput>
void TCompressedVectorView::UnpackTo(TOutput* output)
{
    auto size = GetSize();
    auto width = GetWidth();
    auto input = Ptr_;

    YT_VERIFY(width <= 8 * sizeof(TOutput));

    switch (width) {
        case  0: std::fill(output, output + size, 0); break;
        // Cast to const ui32* produces less instructions.
        #define UNROLLED(width, type) \
            case width: \
                NDetail::UnpackFullyUnrolled<width>(reinterpret_cast<const type*>(input), output, output + size); \
                break;
        #define ALIGNED(width, type) \
            case width: \
                NDetail::UnpackAligned(reinterpret_cast<const type*>(input), output, output + size); \
                break;
        UNROLLED( 1, ui8)
        UNROLLED( 2, ui8)
        UNROLLED( 3, ui32)
        UNROLLED( 4, ui8)
        UNROLLED( 5, ui32)
        UNROLLED( 6, ui32)
        UNROLLED( 7, ui32)

        UNROLLED( 9, ui32)
        UNROLLED(10, ui32)
        UNROLLED(11, ui32)
        UNROLLED(12, ui32)
        UNROLLED(13, ui32)
        UNROLLED(14, ui32)
        UNROLLED(15, ui32)

        UNROLLED(17, ui32)
        UNROLLED(18, ui32)
        UNROLLED(19, ui32)
        UNROLLED(20, ui32)
        UNROLLED(21, ui32)
        UNROLLED(22, ui32)
        UNROLLED(23, ui32)
        UNROLLED(24, ui32)
        UNROLLED(25, ui32)
        UNROLLED(26, ui32)
        UNROLLED(27, ui32)
        UNROLLED(28, ui32)
        UNROLLED(29, ui32)
        UNROLLED(30, ui32)
        UNROLLED(31, ui32)

        ALIGNED( 8, ui8)
        ALIGNED(16, ui16)
        ALIGNED(32, ui32)
        ALIGNED(64, ui64)

        #undef UNROLLED
        #undef ALIGNED
        default:
            NDetail::UnpackValuesFallback(input, output, output + size, width);
            break;
    }
}

template <class TOutput>
void TCompressedVectorView::UnpackTo(TOutput* output, ui32 start, ui32 end)
{
    auto size = GetSize();
    auto width = GetWidth();
    auto input = Ptr_;

    YT_VERIFY(end <= size);

    YT_VERIFY(width <= 8 * sizeof(TOutput));

    if (width == 0) {
        std::fill(output, output + end - start, 0);
    } else {
        switch (width) {
            case  0: std::fill(output, output + end - start, 0); break;
            // Cast to const ui32* produces less instructions.
            #define UNROLLED(Width, type) \
                case Width: \
                    NDetail::UnpackValues<Width>(reinterpret_cast<const type*>(input), output, output + end - start, start); \
                    break;
            #define ALIGNED(width, type) \
                case width: \
                    NDetail::UnpackAligned(reinterpret_cast<const type*>(input) + start, output, output + end - start); \
                    break;
            UNROLLED( 1, ui32)
            UNROLLED( 2, ui8)
            UNROLLED( 3, ui32)
            UNROLLED( 4, ui8)
            UNROLLED( 5, ui32)
            UNROLLED( 6, ui32)
            UNROLLED( 7, ui32)

            UNROLLED( 9, ui64)
            UNROLLED(10, ui64)
            UNROLLED(11, ui64)
            UNROLLED(12, ui64)
            UNROLLED(13, ui64)
            UNROLLED(14, ui64)
            UNROLLED(15, ui64)

            UNROLLED(17, ui64)
            UNROLLED(18, ui64)
            UNROLLED(19, ui64)
            UNROLLED(20, ui64)
            UNROLLED(21, ui64)
            UNROLLED(22, ui64)
            UNROLLED(23, ui64)
            UNROLLED(24, ui64)
            UNROLLED(25, ui64)
            UNROLLED(26, ui64)
            UNROLLED(27, ui64)
            UNROLLED(28, ui64)
            UNROLLED(29, ui64)
            UNROLLED(30, ui64)
            UNROLLED(31, ui64)

            ALIGNED( 8, ui8)
            ALIGNED(16, ui16)
            ALIGNED(32, ui32)
            ALIGNED(64, ui64)

            #undef UNROLLED
            #undef ALIGNED
            default:
                NDetail::UnpackValuesFallback(input, output, output + end - start, width, start);
                break;
        }

#ifndef NDEBUG
        for (int i = 0; i < int(end - start); ++i) {
            YT_VERIFY(TWord(output[i]) == (*this)[i + start]);
        }
#endif
    }
}

template
void TCompressedVectorView::UnpackTo<ui8>(ui8* output);

template
void TCompressedVectorView::UnpackTo<ui16>(ui16* output);

template
void TCompressedVectorView::UnpackTo<ui32>(ui32* output);

template
void TCompressedVectorView::UnpackTo<ui64>(ui64* output);

template
void TCompressedVectorView::UnpackTo<i64>(i64* output);

template
void TCompressedVectorView::UnpackTo<ui8>(ui8* output, ui32 start, ui32 end);

template
void TCompressedVectorView::UnpackTo<ui16>(ui16* output, ui32 start, ui32 end);

template
void TCompressedVectorView::UnpackTo<ui32>(ui32* output, ui32 start, ui32 end);

template
void TCompressedVectorView::UnpackTo<ui64>(ui64* output, ui32 start, ui32 end);

template
void TCompressedVectorView::UnpackTo<i64>(i64* output, ui32 start, ui32 end);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
