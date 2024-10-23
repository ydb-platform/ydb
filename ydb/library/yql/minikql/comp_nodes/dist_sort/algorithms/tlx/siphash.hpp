/*******************************************************************************
 * tlx/siphash.hpp
 *
 * SipHash Implementations borrowed under Public Domain license from
 * https://github.com/floodyberry/siphash
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_SIPHASH_HEADER
#define TLX_SIPHASH_HEADER

#include <tlx/define/attribute_fallthrough.hpp>
#include <tlx/math/bswap_le.hpp>
#include <tlx/math/rol.hpp>

#include <cstdint>
#include <cstdlib>
#include <string>

#if defined(_MSC_VER)

#include <intrin.h>

#if (_MSC_VER > 1200) || defined(_mm_free)
#define __SSE2__
#endif

#endif // !defined(_MSC_VER)

#if defined(__SSE2__)
#include <emmintrin.h>
#endif

namespace tlx {

static inline
std::uint64_t siphash_plain(const std::uint8_t key[16], const std::uint8_t* m, size_t len) {

    std::uint64_t v0, v1, v2, v3;
    std::uint64_t mi, k0, k1;
    std::uint64_t last7;
    size_t i, blocks;

    k0 = bswap64_le(*reinterpret_cast<const std::uint64_t*>(key + 0));
    k1 = bswap64_le(*reinterpret_cast<const std::uint64_t*>(key + 8));
    v0 = k0 ^ 0x736f6d6570736575ull;
    v1 = k1 ^ 0x646f72616e646f6dull;
    v2 = k0 ^ 0x6c7967656e657261ull;
    v3 = k1 ^ 0x7465646279746573ull;

    last7 = static_cast<std::uint64_t>(len & 0xff) << 56;

#define TLX_SIPCOMPRESS() \
    v0 += v1; v2 += v3;   \
    v1 = rol64(v1, 13);   \
    v3 = rol64(v3, 16);   \
    v1 ^= v0; v3 ^= v2;   \
    v0 = rol64(v0, 32);   \
    v2 += v1; v0 += v3;   \
    v1 = rol64(v1, 17);   \
    v3 = rol64(v3, 21);   \
    v1 ^= v2; v3 ^= v0;   \
    v2 = rol64(v2, 32);

    for (i = 0, blocks = (len & ~7); i < blocks; i += 8) {
        mi = bswap64_le(*reinterpret_cast<const std::uint64_t*>(m + i));
        v3 ^= mi;
        TLX_SIPCOMPRESS();
        TLX_SIPCOMPRESS();
        v0 ^= mi;
    }

    switch (len - blocks) {
    case 7:
        last7 |= static_cast<std::uint64_t>(m[i + 6]) << 48;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 6:
        last7 |= static_cast<std::uint64_t>(m[i + 5]) << 40;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 5:
        last7 |= static_cast<std::uint64_t>(m[i + 4]) << 32;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 4:
        last7 |= static_cast<std::uint64_t>(m[i + 3]) << 24;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 3:
        last7 |= static_cast<std::uint64_t>(m[i + 2]) << 16;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 2:
        last7 |= static_cast<std::uint64_t>(m[i + 1]) << 8;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 1:
        last7 |= static_cast<std::uint64_t>(m[i + 0]);
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 0:
    default:;
    }

    v3 ^= last7;
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();
    v0 ^= last7;
    v2 ^= 0xff;
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();

#undef TLX_SIPCOMPRESS

    return v0 ^ v1 ^ v2 ^ v3;
}

/******************************************************************************/
// SSE2 vectorization

#if defined(__SSE2__)

union siphash_packedelem64 {
    std::uint64_t u[2];
    __m128i v;
};

/* 0,2,1,3 */
static const siphash_packedelem64 siphash_init[2] = {
    {
        { 0x736f6d6570736575ull, 0x6c7967656e657261ull }
    },
    {
        { 0x646f72616e646f6dull, 0x7465646279746573ull }
    }
};

static const siphash_packedelem64 siphash_final = {
    { 0x0000000000000000ull, 0x00000000000000ffull }
};

static inline
std::uint64_t siphash_sse2(const std::uint8_t key[16], const std::uint8_t* m, size_t len) {

    __m128i k, v02, v20, v13, v11, v33, mi;
    std::uint64_t last7;
    std::uint32_t lo, hi;
    size_t i, blocks;

    k = _mm_loadu_si128(reinterpret_cast<const __m128i*>(key + 0));
    v02 = siphash_init[0].v;
    v13 = siphash_init[1].v;
    v02 = _mm_xor_si128(v02, _mm_unpacklo_epi64(k, k));
    v13 = _mm_xor_si128(v13, _mm_unpackhi_epi64(k, k));

    last7 = static_cast<std::uint64_t>(len & 0xff) << 56;

#define TLX_SIPCOMPRESS()                                                      \
    v11 = v13;                                                                 \
    v33 = _mm_shuffle_epi32(v13, _MM_SHUFFLE(1, 0, 3, 2));                     \
    v11 = _mm_or_si128(_mm_slli_epi64(v11, 13), _mm_srli_epi64(v11, 64 - 13)); \
    v02 = _mm_add_epi64(v02, v13);                                             \
    v33 = _mm_shufflelo_epi16(v33, _MM_SHUFFLE(2, 1, 0, 3));                   \
    v13 = _mm_unpacklo_epi64(v11, v33);                                        \
    v13 = _mm_xor_si128(v13, v02);                                             \
    v20 = _mm_shuffle_epi32(v02, _MM_SHUFFLE(0, 1, 3, 2));                     \
    v11 = v13;                                                                 \
    v33 = _mm_shuffle_epi32(v13, _MM_SHUFFLE(1, 0, 3, 2));                     \
    v11 = _mm_or_si128(_mm_slli_epi64(v11, 17), _mm_srli_epi64(v11, 64 - 17)); \
    v20 = _mm_add_epi64(v20, v13);                                             \
    v33 = _mm_or_si128(_mm_slli_epi64(v33, 21), _mm_srli_epi64(v33, 64 - 21)); \
    v13 = _mm_unpacklo_epi64(v11, v33);                                        \
    v02 = _mm_shuffle_epi32(v20, _MM_SHUFFLE(0, 1, 3, 2));                     \
    v13 = _mm_xor_si128(v13, v20);

    for (i = 0, blocks = (len & ~7); i < blocks; i += 8) {
        mi = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(m + i));
        v13 = _mm_xor_si128(v13, _mm_slli_si128(mi, 8));
        TLX_SIPCOMPRESS();
        TLX_SIPCOMPRESS();
        v02 = _mm_xor_si128(v02, mi);
    }

    switch (len - blocks) {
    case 7:
        last7 |= static_cast<std::uint64_t>(m[i + 6]) << 48;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 6:
        last7 |= static_cast<std::uint64_t>(m[i + 5]) << 40;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 5:
        last7 |= static_cast<std::uint64_t>(m[i + 4]) << 32;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 4:
        last7 |= static_cast<std::uint64_t>(m[i + 3]) << 24;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 3:
        last7 |= static_cast<std::uint64_t>(m[i + 2]) << 16;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 2:
        last7 |= static_cast<std::uint64_t>(m[i + 1]) << 8;
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 1:
        last7 |= static_cast<std::uint64_t>(m[i + 0]);
        TLX_ATTRIBUTE_FALLTHROUGH;
    case 0:
    default:;
    }

    mi = _mm_unpacklo_epi32(
        _mm_cvtsi32_si128(static_cast<std::uint32_t>(last7)),
        _mm_cvtsi32_si128(static_cast<std::uint32_t>(last7 >> 32)));
    v13 = _mm_xor_si128(v13, _mm_slli_si128(mi, 8));
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();
    v02 = _mm_xor_si128(v02, mi);
    v02 = _mm_xor_si128(v02, siphash_final.v);
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();
    TLX_SIPCOMPRESS();

    v02 = _mm_xor_si128(v02, v13);
    v02 = _mm_xor_si128(v02, _mm_shuffle_epi32(v02, _MM_SHUFFLE(1, 0, 3, 2)));
    lo = _mm_cvtsi128_si32(v02);
    hi = _mm_cvtsi128_si32(_mm_srli_si128(v02, 4));

#undef TLX_SIPCOMPRESS

    return (static_cast<std::uint64_t>(hi) << 32) | lo;
}

#endif  // defined(__SSE2__)

/******************************************************************************/
// Switch between available implementations

static inline
std::uint64_t siphash(const std::uint8_t key[16], const std::uint8_t* msg, size_t size) {
#if defined(__SSE2__)
    return siphash_sse2(key, msg, size);
#else
    return siphash_plain(key, msg, size);
#endif
}

static inline
std::uint64_t siphash(const std::uint8_t* msg, size_t size) {
    const unsigned char key[16] = {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    };
    return siphash(key, msg, size);
}

static inline
std::uint64_t siphash(const char* msg, size_t size) {
    return siphash(reinterpret_cast<const std::uint8_t*>(msg), size);
}

static inline
std::uint64_t siphash(const std::string& str) {
    return siphash(str.data(), str.size());
}

template <typename Type>
static inline
std::uint64_t siphash(const Type& value) {
    return siphash(reinterpret_cast<const std::uint8_t*>(&value), sizeof(value));
}

#undef rol64

} // namespace tlx

#endif // !TLX_SIPHASH_HEADER

/******************************************************************************/
