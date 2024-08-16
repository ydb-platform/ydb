#include "yql_decimal.h"

#include <cstring>
#include <ostream>
#include <string>

namespace NYql {
namespace NDecimal {

static const TUint128 Ten(10U);

TUint128 GetDivider(ui8 scale) {
    TUint128 d(1U);
    while (scale--)
        d *= Ten;
    return d;
}

bool IsError(TInt128 v) {
    return v > Nan() || v < -Nan();
}

bool IsNan(TInt128 v) {
    return v == Nan() || v == -Nan();
}

bool IsInf(TInt128 v) {
    return v == Inf() || v == -Inf();
}

bool IsNormal(TInt128 v) {
    return v < Inf() && v > -Inf();
}

bool IsComparable(TInt128 v) {
    return v <= Inf() && v >= -Inf();
}

const char* ToString(TInt128 val, ui8 precision, ui8 scale) {
    if (!precision || precision > MaxPrecision || scale > precision) {
        return nullptr;
    }

    if (val == Inf())
        return "inf";
    if (val == -Inf())
        return "-inf";
    if (val == Nan())
        return "nan";
    if (val == -Nan())
        return "-nan";

    if (!IsNormal(val)) {
        return nullptr;
    }

    if (!val) {
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
        if (!precision--) {
            return nullptr;
        }


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
            if (!precision--) {
                return nullptr;
            }

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


TInt128 FromString(const TStringBuf& str, ui8 precision, ui8 scale) {
    if (scale > precision)
        return Err();

    auto s = str.data();
    auto l = str.size();

    if (!s || !l)
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

    for (bool dot = false; l; --l) {
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

        if (!dot && v && !integral--) {
            return neg ? -Inf() : Inf();
        }
    }

    if (l--) {
        const char c = *s++;
        if (!std::isdigit(c))
            return Err();

        bool plus = c > '5';
        if (!plus && c == '5') {
            for (plus = v & 1; !plus && l; --l) {
                const char c = *s++;
                if (!std::isdigit(c))
                    return Err();

                plus = c != '0';
            }
        }

        while (l--)
            if (!std::isdigit(*s++))
                return Err();

        if (plus)
            if (++v >= GetDivider(precision))
                v = Inf();
    }

    while (scale--)
        v *= Ten;

    return neg ? -v : v;
}

TInt128 FromStringEx(const TStringBuf& str, ui8 precision, ui8 scale) {
    if (scale > precision)
        return Err();

    const auto s = str.data();

    for (auto ptr = s + str.size() - 1U; ptr > s; --ptr) {
        if (*ptr == 'E' || *ptr == 'e') {
            const auto len = ptr - s;
            if (!len)
                return Err();

            const auto exp = std::atoi(++ptr);
            if (!exp)
                return Err();

            const int p = precision, s = int(scale) + exp;

            const auto r = exp > 0 ?
                FromString(str.Head(len), precision, std::min(s, p)):
                FromString(str.Head(len), std::min(p - exp, int(MaxPrecision)), std::max(s, 0));

            if (IsNan(r)) {
                return Err();
            }

            if (const auto e = exp > 0 ? std::max(0, s - p) : std::min(0, s)) {
                if (r && IsNormal(r)) {
                    if (exp > 0)
                        return Mul(r, GetDivider(+e));
                    if (exp < 0)
                        return Div(r, GetDivider(-e));
                }
            }

            return r;
        }
    }

    return FromString(str, precision, scale);
}

bool IsValid(const TStringBuf& str) {
    auto s = str.data();
    auto l = str.size();

    if (!s || !l)
        return false;

    if ('-' == *s || '+' == *s) {
        ++s;
        --l;
    }

    if (3U == l && (IsInf(s) || IsNan(s))) {
        return true;
    }

    for (bool dot = false; l--;) {
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

TInt128 Mod(TInt128 a, TInt128 b) {
    if (!b || !(IsNormal(a) && IsNormal(b)))
        return Nan();

    return a % b;
}

TInt128 Div(TInt128 a, TInt128 b) {
    if (IsNan(a) || IsNan(b))
        return Nan();

    if (!b) {
        if (a > 0)
            return Inf();
        else if (a < 0)
            return -Inf();
        else
            return Nan();
    } else if (IsInf(b)) {
        return IsInf(a) ? Nan() : TInt128(0);
    } else if (IsInf(a)) {
        return b > 0 ? a : -a;
    }

    if (b & 1)
        a = TUint128(a) << 1U;
    else
        b >>= 1;

    auto d = a / b;

    if (d & 1) {
        if (const auto m = a % b) {
            if (m > 0) ++d;
            // else --d;
        } else {
            if (d & 2) ++d;
        }
    }

    return d >>= 1;
}

namespace {

using TInt256 = TWide<TInt128, TInt128, TUint128>;

TInt128 Normalize(const TInt256& v) {
    static const TInt256 PInf256(+Inf()), NInf256(-Inf());

    if (v > PInf256)
        return +Inf();
    if (v < NInf256)
        return -Inf();
    return *reinterpret_cast<const TInt128*>(&v);
}

constexpr auto HalfBitSize = sizeof(TUint128) << 2U;

TUint128 GetUpperHalf(const TUint128& v) {
    return v >> HalfBitSize;
}

TUint128 GetLowerHalf(const TUint128& v) {
    return v & TUint128(0xFFFFFFFFFFFFFFFFULL);
}

TInt256 WidenMul(const TInt128& lhs, const TInt128& rhs) {
    const bool nl = lhs < 0;
    const bool nr = rhs < 0;

    const TUint128 l = nl ? -lhs : +lhs;
    const TUint128 r = nr ? -rhs : +rhs;

    const TUint128 lh[] = {GetLowerHalf(l), GetUpperHalf(l)};
    const TUint128 rh[] = {GetLowerHalf(r), GetUpperHalf(r)};

    const TUint128 prods[] = {lh[0] * rh[0], lh[0] * rh[1], lh[1] * rh[0], lh[1] * rh[1]};

    const TUint128 fourthQ = GetLowerHalf(prods[0]);
    const TUint128 thirdQ = GetUpperHalf(prods[0]) + GetLowerHalf(prods[1]) + GetLowerHalf(prods[2]);
    const TUint128 secondQ = GetUpperHalf(thirdQ) + GetUpperHalf(prods[1]) + GetUpperHalf(prods[2]) + GetLowerHalf(prods[3]);
    const TUint128 firstQ = GetUpperHalf(secondQ) + GetUpperHalf(prods[3]);

    const TInt256 combine((firstQ << HalfBitSize) | GetLowerHalf(secondQ), (thirdQ << HalfBitSize) | fourthQ);
    return nl == nr ? +combine : -combine;
}

template<bool MayOddDivider>
TInt256 Div(TInt256&& a, TInt256&& b) {
    if (MayOddDivider && b & 1)
        a <<= 1;
    else
        b >>= 1;

    auto d = a / b;

    if (d & 1) {
        if (const auto m = a % b) {
            if (m > 0) ++d;
            // else --d;
        } else {
            if (d & 2) ++d;
        }
    }

    return d >>= 1;
}

}

TInt128 Mul(TInt128 a, TInt128 b) {
    if (IsNan(a) || IsNan(b))
        return Nan();

    if (IsInf(a))
        return !b ? Nan() : (b > 0 ? a : -a);

    if (IsInf(b))
        return !a ? Nan() : (a > 0 ? b : -b);

    return Normalize(WidenMul(a, b));
}

TInt128 MulAndDivNormalMultiplier(TInt128 a, TInt128 b, TInt128 c) {
    if (IsNan(a) || IsNan(c))
        return Nan();

    if (!c) {
        if (a > 0)
            return Inf();
        else if (a < 0)
            return -Inf();
        else
            return Nan();
    } else if (IsInf(c)) {
        return IsInf(a) ? Nan() : TInt128(0);
    } else if (IsInf(a)) {
        return c > 0 ? a : -a;
    }

    return Normalize(Div<true>(WidenMul(a, b), TInt256(c)));
}

TInt128 MulAndDivNormalDivider(TInt128 a, TInt128 b, TInt128 c) {
    if (IsNan(a) || IsNan(b))
        return Nan();

    if (IsInf(a))
        return !b ? Nan() : (b > 0 ? a : -a);

    if (IsInf(b))
        return !a ? Nan() : (a > 0 ? b : -b);

    return Normalize(Div<false>(WidenMul(a, b), TInt256(c)));
}

}
}
