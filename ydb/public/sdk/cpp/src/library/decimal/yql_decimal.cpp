#include "yql_decimal.h"

#include <cstring>
#include <ostream>
#include <string>

namespace NYdb {
inline namespace Dev {
namespace NDecimal {

static const TUint128 Ten(10U);

TUint128 GetDivider(ui8 scale) {
    TUint128 d(1U);
    while (scale--)
        d *= Ten;
    return d;
}

bool IsError(TInt128 v) {
    return v > Nan() || v < -Inf();
}

bool IsNan(TInt128 v) {
    return v == Nan();
}

bool IsInf(TInt128 v) {
    return v == Inf() || v == -Inf();
}

bool IsNormal(TInt128 v) {
    return v < Inf() && v > -Inf();
}

const char* ToString(TInt128 val, ui8 precision, ui8 scale) {
    if (precision == 0 || precision > MaxPrecision || scale > precision) {
        return "";
    }

    if (val == Inf())
        return "inf";
    if (val == -Inf())
        return "-inf";
    if (val == Nan())
        return "nan";

    if (!IsNormal(val)) {
        return nullptr;
    }

    if (val == 0) {
        return "0";
    }

    const bool neg = val < 0;
    TUint128 v = neg ? -val : val;

    // log_{10}(2^120) ~= 36.12, 37 decimal places
    // plus dot, zero before dot, sign and zero byte at the end
    static thread_local char str[40];
    auto end = str + sizeof(str);
    *--end = 0;

    auto s = end;

    do {
        if (precision == 0) {
            return "";
        }
        --precision;


        const auto digit = ui8(v % Ten);
        if (digit || !scale || s != end) {
            *--s = "0123456789"[digit];
        }

        if (scale && !--scale && s != end) {
            *--s = '.';
        }
    } while (v /= Ten);

    if (scale) {
        do {
            if (precision == 0) {
                return nullptr;
            }
            --precision;

            *--s = '0';
        } while (--scale);

        *--s = '.';
    }

    if (*s == '.') {
        *--s = '0';
    }

    if (neg) {
        *--s = '-';
    }

    return s;
}

namespace {
    bool IsNan(const char* s) {
        return (s[0] == 'N' || s[0] == 'n') && (s[1] == 'A' || s[1] == 'a') && (s[2] == 'N' || s[2] == 'n');
    }

    bool IsInf(const char* s) {
        return (s[0] == 'I' || s[0] == 'i') && (s[1] == 'N' || s[1] == 'n') && (s[2] == 'F' || s[2] == 'f');
    }
}


TInt128 FromString(const std::string_view& str, ui8 precision, ui8 scale) {
    if (scale > precision)
        return Err();

    auto s = str.data();
    auto l = str.size();

    if (s == nullptr || l == 0)
        return Err();

    const bool neg = '-' == *s;
    if (neg || '+' == *s) {
        ++s;
        --l;
    }

    if (3U == l) {
        if (IsInf(s))
            return neg ? -Inf() : Inf();
        if (IsNan(s))
            return Nan();
    }

    TUint128 v = 0U;
    auto integral = precision - scale;

    for (bool dot = false; l > 0; --l) {
        if (*s == '.') {
            if (dot)
                return Err();

            ++s;
            dot = true;
            continue;
        }

        if (dot) {
            if (scale)
                --scale;
            else
                break;
        }

        const char c = *s++;
        if (!std::isdigit(c))
            return Err();

        v *= Ten;
        v += c - '0';

        if (!dot && v > 0) {
            if (integral == 0) {
                return neg ? -Inf() : Inf();
            }
            --integral;
        }
    }

    if (l > 0) {
        --l;
        const char c = *s++;
        if (!std::isdigit(c))
            return Err();

        bool plus = c > '5';
        if (!plus && c == '5') {
            for (plus = v & 1; !plus && l > 0; --l) {
                const char c = *s++;
                if (!std::isdigit(c))
                    return Err();

                plus = c != '0';
            }
        }

        while (l > 0) {
            --l;
            if (!std::isdigit(*s++))
                return Err();
        }

        if (plus)
            if (++v >= GetDivider(precision))
                v = Inf();
    }

    while (scale > 0) {
        --scale;
        v *= Ten;
    }

    return neg ? -v : v;
}

TInt128 FromHalfs(ui64 lo, i64 hi) {
    ui64 half[2] = {lo, static_cast<ui64>(hi)};
    TInt128 val128;
    std::memcpy(&val128, half, sizeof(val128));
    return val128;
}

bool IsValid(const std::string_view& str) {
    auto s = str.data();
    auto l = str.size();

    if (s == nullptr || l == 0)
        return false;

    if ('-' == *s || '+' == *s) {
        ++s;
        --l;
    }

    if (3U == l && (IsInf(s) || IsNan(s))) {
        return true;
    }

    for (bool dot = false; l > 0; l--) {
        const char c = *s++;
        if (c == '.') {
            if (dot)
                return false;

            dot = true;
            continue;
        }

        if (!std::isdigit(c))
            return false;
    }

    return true;
}

}
}
}
