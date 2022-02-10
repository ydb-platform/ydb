#pragma once

#include <util/generic/bitops.h>
#include <util/generic/typetraits.h>
#include <util/system/yassert.h>

template <typename TWord>
struct TBitSeqTraits {
    static constexpr ui8 NumBits = CHAR_BIT * sizeof(TWord);
    static constexpr TWord ModMask = static_cast<TWord>(NumBits - 1);
    static constexpr TWord DivShift = MostSignificantBitCT(NumBits);

    static inline TWord ElemMask(ui8 count) {
        // NOTE: Shifting by the type's length is UB, so we need this workaround.
        if (Y_LIKELY(count))
            return TWord(-1) >> (NumBits - count);
        return 0;
    }

    static inline TWord BitMask(ui8 pos) {
        return TWord(1) << pos;
    }

    static size_t NumOfWords(size_t bits) {
        return (bits + NumBits - 1) >> DivShift;
    }

    static bool Test(const TWord* data, ui64 pos, ui64 size) {
        Y_ASSERT(pos < size);
        return data[pos >> DivShift] & BitMask(pos & ModMask);
    }

    static TWord Get(const TWord* data, ui64 pos, ui8 width, TWord mask, ui64 size) {
        if (!width)
            return 0;
        Y_ASSERT((pos + width) <= size);
        size_t word = pos >> DivShift;
        TWord shift1 = pos & ModMask;
        TWord shift2 = NumBits - shift1;
        TWord res = data[word] >> shift1 & mask;
        if (shift2 < width) {
            res |= data[word + 1] << shift2 & mask;
        }
        return res;
    }

    static_assert(std::is_unsigned<TWord>::value, "Expected std::is_unsigned<T>::value.");
    static_assert((NumBits & (NumBits - 1)) == 0, "NumBits should be a power of 2.");
};
