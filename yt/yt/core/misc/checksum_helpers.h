#pragma once

#include "public.h"

#ifdef YT_USE_SSE42
    #include <tmmintrin.h>
    #include <nmmintrin.h>
    #include <wmmintrin.h>
#endif

namespace NYT::NCrc {

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_USE_SSE42

inline __m128i _mm_shift_right_si128(__m128i v, ui8 offset)
{
    static const ui8 RotateMask[] = {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80
    };

    return _mm_shuffle_epi8(v, _mm_loadu_si128((__m128i *) (RotateMask + offset)));
}

inline __m128i _mm_shift_left_si128(__m128i v, ui8 offset)
{
    static const ui8 RotateMask[] = {
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f
    };

    return _mm_shuffle_epi8(v, _mm_loadu_si128((__m128i *) (RotateMask + 16 - offset)));
}

inline __m128i ReverseBytes(__m128i value)
{
    return _mm_shuffle_epi8(value,
        _mm_setr_epi8(0xf, 0xe, 0xd, 0xc, 0xb, 0xa, 0x9, 0x8, 0x7, 0x6, 0x5, 0x4, 0x3, 0x2, 0x1, 0x0));
}

inline __m128i Fold(__m128i value, __m128i foldFactor)
{
    __m128i high = _mm_clmulepi64_si128(value, foldFactor, 0x11);
    __m128i low = _mm_clmulepi64_si128(value, foldFactor, 0x00);
    return _mm_xor_si128(high, low);
}

inline __m128i Fold(__m128i value, __m128i data, __m128i foldFactor)
{
    return _mm_xor_si128(data, Fold(value, foldFactor));
}

YT_ATTRIBUTE_NO_SANITIZE_ADDRESS inline __m128i AlignedPrefixLoad(const void* p, size_t* length)
{
    size_t offset = (size_t)p & 15; *length = 16 - offset;
    return _mm_shift_right_si128(_mm_load_si128((__m128i*)((char*)p - offset)), offset);
}

YT_ATTRIBUTE_NO_SANITIZE_ADDRESS inline __m128i UnalignedLoad(const void* buf, size_t expectedLength = 16)
{
    size_t length;
    __m128i result = AlignedPrefixLoad(buf, &length);

    if (length < expectedLength) {
        result = _mm_loadu_si128((__m128i*) buf);
    }

    return ReverseBytes(result);
}

inline __m128i AlignedLoad(const void* buf)
{
    return ReverseBytes(_mm_load_si128((__m128i*) buf));
}

inline __m128i FoldTail(__m128i result, __m128i tail, __m128i foldFactor, size_t tailLength)
{
    tail = _mm_or_si128(_mm_shift_right_si128(tail, 16 - tailLength), _mm_shift_left_si128(result, tailLength));
    result = _mm_shift_right_si128(result, 16 - tailLength);
    return Fold(result, tail, foldFactor);
}

static ui64 BarretReduction(__m128i chunk, ui64 poly, ui64 mu)
{
    __m128i muAndPoly = _mm_set_epi64x(poly, mu);
    __m128i high = _mm_set_epi64x(_mm_cvtsi128_si64(_mm_srli_si128(chunk, 8)), 0);

    // T1(x) = (R(x) div x^64) clmul mu
    // mu is 65 bit polynomial
    __m128i t1 = _mm_xor_si128(_mm_clmulepi64_si128(high, muAndPoly, 0x01), high);

    // T2(x) = (T1(x) div x^64) clmul p(x)
    // p(x) is 65 bit polynomial, so we have to do xor of high 64 bits after carry-less multiplication
    // but since the next operation is (R(x) xor T2(x)) mod x^64, xor is unnecessary
    __m128i t2 = _mm_clmulepi64_si128(t1, muAndPoly, 0x11);

    // return (R(x) xor T2(x)) mod x^64
    return _mm_cvtsi128_si64(_mm_xor_si128(chunk, t2)); // .m128i_u64[0];
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrc
