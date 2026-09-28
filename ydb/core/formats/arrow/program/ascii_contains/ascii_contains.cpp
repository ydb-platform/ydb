#include "ascii_contains.h"

#include <util/string/ascii.h>
#include <util/system/compiler.h>

#include <algorithm>
#include <cstring>

namespace NKikimr::NArrow::NSSA {
namespace {

constexpr size_t MemchrMinHaystack = 32;

bool IgnoreCaseEquals(char a, char b) noexcept {
    return AsciiToUpper(a) == AsciiToUpper(b);
}

bool AsciiContainsIgnoreCaseScalar(const TStringBuf haystack, const TStringBuf needle) noexcept {
    return std::search(haystack.begin(), haystack.end(), needle.begin(), needle.end(), IgnoreCaseEquals) != haystack.end();
}

const char* FindFirstIgnoreCaseChar(const char* begin, const char* end, const char lo, const char up) noexcept {
    const size_t len = static_cast<size_t>(end - begin);
    if (len == 0) {
        return nullptr;
    }
    if (lo == up) {
        return static_cast<const char*>(std::memchr(begin, lo, len));
    }

    const char* pLo = static_cast<const char*>(std::memchr(begin, lo, len));
    if (pLo == begin) {
        return pLo;
    }
    if (pLo) {
        const char* pUp = static_cast<const char*>(std::memchr(begin, up, static_cast<size_t>(pLo - begin)));
        return pUp ? pUp : pLo;
    }
    return static_cast<const char*>(std::memchr(begin, up, len));
}

} // namespace

bool AsciiContainsIgnoreCaseMemchr(const TStringBuf haystack, const TStringBuf needle) noexcept {
    const size_t m = needle.size();
    if (Y_UNLIKELY(m == 0)) {
        return true;
    }

    const size_t n = haystack.size();
    if (m > n) {
        return false;
    }

    if (n <= MemchrMinHaystack) {
        return AsciiContainsIgnoreCaseScalar(haystack, needle);
    }

    const char* const hay = haystack.data();
    const char* const nee = needle.data();
    const char* const last = hay + (n - m) + 1; // exclusive end of valid start positions
    const char lo = AsciiToLower(nee[0]);
    const char up = AsciiToUpper(nee[0]);

    const char* p = hay;
    while (p < last) {
        const char* candidate = FindFirstIgnoreCaseChar(p, last, lo, up);
        if (!candidate) {
            return false;
        }

        bool match = true;
        for (size_t i = 1; i < m; ++i) {
            if (AsciiToUpper(candidate[i]) != AsciiToUpper(nee[i])) {
                match = false;
                break;
            }
        }
        if (match) {
            return true;
        }
        p = candidate + 1;
    }
    return false;
}

} // namespace NKikimr::NArrow::NSSA
