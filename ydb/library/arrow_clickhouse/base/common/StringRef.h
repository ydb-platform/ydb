#pragma once

#include <cassert>
#include <stdexcept> // for std::logic_error
#include <string>
#include <vector>
#include <functional>
#include <iosfwd>

#include <common/types.h>
#include <common/unaligned.h>

#if defined(__SSE2__)
    #include <emmintrin.h>
#endif

#if defined(__SSE4_2__)
    #include <smmintrin.h>
    #include <nmmintrin.h>
#endif

namespace CH
{

/**
 * The std::string_view-like container to avoid creating strings to find substrings in the hash table.
 */
struct StringRef
{
    const char * data = nullptr;
    size_t size = 0;

    /// Non-constexpr due to reinterpret_cast.
    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    StringRef(const CharT * data_, size_t size_) : data(reinterpret_cast<const char *>(data_)), size(size_)
    {
        /// Sanity check for overflowed values.
        assert(size < 0x8000000000000000ULL);
    }

    constexpr StringRef(const char * data_, size_t size_) : data(data_), size(size_) {}

    StringRef(const std::string & s) : data(s.data()), size(s.size()) {}
    constexpr explicit StringRef(std::string_view s) : data(s.data()), size(s.size()) {}
    constexpr StringRef(const char * data_) : StringRef(std::string_view{data_}) {}
    constexpr StringRef() = default;

    std::string toString() const { return std::string(data, size); }

    explicit operator std::string() const { return toString(); }
    constexpr explicit operator std::string_view() const { return {data, size}; }
};

/// Here constexpr doesn't implicate inline, see https://www.viva64.com/en/w/v1043/
/// nullptr can't be used because the StringRef values are used in SipHash's pointer arithmetic
/// and the UBSan thinks that something like nullptr + 8 is UB.
constexpr const inline char empty_string_ref_addr{};
constexpr const inline StringRef EMPTY_STRING_REF{&empty_string_ref_addr, 0};

using StringRefs = std::vector<StringRef>;


#if defined(__SSE2__)

/** Compare strings for equality.
  * The approach is controversial and does not win in all cases.
  * For more information, see hash_map_string_2.cpp
  */

inline bool compareSSE2(const char * p1, const char * p2)
{
    return 0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))));
}

inline bool compareSSE2x4(const char * p1, const char * p2)
{
    return 0xFFFF == _mm_movemask_epi8(
        _mm_and_si128(
            _mm_and_si128(
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))),
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 1),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 1))),
            _mm_and_si128(
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 2),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 2)),
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 3),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 3)))));
}

inline bool memequalSSE2Wide(const char * p1, const char * p2, size_t size)
{
    while (size >= 64)
    {
        if (compareSSE2x4(p1, p2))
        {
            p1 += 64;
            p2 += 64;
            size -= 64;
        }
        else
            return false;
    }

    switch ((size % 64) / 16)
    {
        case 3: if (!compareSSE2(p1 + 32, p2 + 32)) return false; [[fallthrough]];
        case 2: if (!compareSSE2(p1 + 16, p2 + 16)) return false; [[fallthrough]];
        case 1: if (!compareSSE2(p1     , p2     )) return false; [[fallthrough]];
        case 0: break;
    }

    p1 += (size % 64) / 16 * 16;
    p2 += (size % 64) / 16 * 16;

    switch (size % 16)
    {
        case 15: if (p1[14] != p2[14]) return false; [[fallthrough]];
        case 14: if (p1[13] != p2[13]) return false; [[fallthrough]];
        case 13: if (p1[12] != p2[12]) return false; [[fallthrough]];
        case 12: if (unalignedLoad<uint32_t>(p1 + 8) == unalignedLoad<uint32_t>(p2 + 8)) goto l8; else return false;
        case 11: if (p1[10] != p2[10]) return false; [[fallthrough]];
        case 10: if (p1[9] != p2[9]) return false; [[fallthrough]];
        case 9:  if (p1[8] != p2[8]) return false;
        l8: [[fallthrough]];
        case 8:  return unalignedLoad<uint64_t>(p1) == unalignedLoad<uint64_t>(p2);
        case 7:  if (p1[6] != p2[6]) return false; [[fallthrough]];
        case 6:  if (p1[5] != p2[5]) return false; [[fallthrough]];
        case 5:  if (p1[4] != p2[4]) return false; [[fallthrough]];
        case 4:  return unalignedLoad<uint32_t>(p1) == unalignedLoad<uint32_t>(p2);
        case 3:  if (p1[2] != p2[2]) return false; [[fallthrough]];
        case 2:  return unalignedLoad<uint16_t>(p1) == unalignedLoad<uint16_t>(p2);
        case 1:  if (p1[0] != p2[0]) return false; [[fallthrough]];
        case 0:  break;
    }

    return true;
}

#endif


inline bool operator== (StringRef lhs, StringRef rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

#if defined(__SSE2__)
    return memequalSSE2Wide(lhs.data, rhs.data, lhs.size);
#else
    return 0 == memcmp(lhs.data, rhs.data, lhs.size);
#endif
}

inline bool operator!= (StringRef lhs, StringRef rhs)
{
    return !(lhs == rhs);
}

inline bool operator< (StringRef lhs, StringRef rhs)
{
    int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
    return cmp < 0 || (cmp == 0 && lhs.size < rhs.size);
}

inline bool operator> (StringRef lhs, StringRef rhs)
{
    int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
    return cmp > 0 || (cmp == 0 && lhs.size > rhs.size);
}

struct StringRefHash {
    size_t operator() (StringRef x) const
    {
        return  std::hash<std::string_view>()(std::string_view(x));
    }
};

}

namespace std
{
    template <>
    struct hash<CH::StringRef> : public CH::StringRefHash {};
}


namespace ZeroTraits
{
    inline bool check(const CH::StringRef & x) { return 0 == x.size; }
    inline void set(CH::StringRef & x) { x.size = 0; }
}


std::ostream & operator<<(std::ostream & os, const CH::StringRef & str);
