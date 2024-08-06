#pragma once

#include <util/generic/strbuf.h>
#include "yql_wide_int.h"

#include <type_traits>
#include <limits>

namespace NYql {
namespace NDecimal {

#ifdef _win_
#ifndef DONT_USE_NATIVE_INT128
#define DONT_USE_NATIVE_INT128
#endif
#endif

#ifdef DONT_USE_NATIVE_INT128
using TInt128 = TWide<i64>;
using TUint128 = TWide<ui64>;
#else
using TInt128 = signed __int128;
using TUint128 = unsigned __int128;
#endif

template<ui8 Scale> struct TDivider;
#if defined(__clang__) && defined(DONT_USE_NATIVE_INT128)
template<> struct TDivider<0> { static inline constexpr TUint128 Value = 1U; };
template<ui8 Scale> struct TDivider { static inline constexpr TInt128 Value = TDivider<Scale - 1U>::Value * 10U; };
#else
template<> struct TDivider<0> { static constexpr TUint128 Value = 1U; };
template<ui8 Scale> struct TDivider { static constexpr TUint128 Value = TDivider<Scale - 1U>::Value * 10U; };
#endif

constexpr ui8 MaxPrecision = 35;

static_assert(sizeof(TInt128) == 16, "Wrong size of TInt128, expected 16");

inline constexpr TInt128 Inf() {
    return TInt128(100000000000000000ULL) * TInt128(1000000000000000000ULL);
}

inline constexpr TInt128 Nan() {
    return Inf() + TInt128(1);
}

inline constexpr TInt128 Err() {
    return Nan() + TInt128(1);
}

TUint128 GetDivider(ui8 scale);

template<ui8 Precision>
inline constexpr TUint128 GetDivider() {
    return TDivider<Precision>::Value;
}

template<ui8 Precision, bool IncLow = false, bool DecHigh = false>
inline constexpr std::pair<TInt128, TInt128> GetBounds() {
    return std::make_pair(-GetDivider<Precision>() + (IncLow ? 1 : 0), +GetDivider<Precision>() - (DecHigh ? 1 : 0));
}

bool IsError(TInt128 v);
bool IsNan(TInt128 v);
bool IsInf(TInt128 v);

bool IsNormal(TInt128 v);
bool IsComparable(TInt128 v);

template<ui8 Precision>
inline bool IsNormal(TInt128 v) {
    const auto& b = GetBounds<Precision>();
    return v > b.first && v < b.second;
}

const char* ToString(TInt128 v, ui8 precision, ui8 scale = 0);
TInt128 FromString(const TStringBuf& str, ui8 precision, ui8 scale = 0);

// Accept string representation with exponent.
TInt128 FromStringEx(const TStringBuf& str, ui8 precision, ui8 scale);

template<typename TMkqlProto>
inline TInt128 FromProto(const TMkqlProto& val) {
    ui64 half[2] = {val.GetLow128(), val.GetHi128()};
    TInt128 val128;
    std::memcpy(&val128, half, sizeof(val128));
    return val128;
}

template<typename TValue>
inline constexpr TValue YtDecimalNan() {
    return std::numeric_limits<TValue>::max();
}

template<>
inline constexpr TInt128 YtDecimalNan<TInt128>() {
    return ~(TInt128(1) << 127);
}

template<typename TValue>
inline constexpr TValue YtDecimalInf() {
    return YtDecimalNan<TValue>() - 1;
}

template<typename TValue>
inline TInt128 FromYtDecimal(TValue val) {
    static_assert(std::is_same<TInt128, TValue>::value || std::is_signed<TValue>::value, "Expected signed value");
    if (YtDecimalNan<TValue>() == val) {
        return Nan();
    } else if (YtDecimalInf<TValue>() == val) {
        return Inf();
    } else if (-YtDecimalInf<TValue>() == val) {
        return -Inf();
    } else {
        return TInt128(val);
    }
}

template<typename TValue>
inline TValue ToYtDecimal(TInt128 val) {
    static_assert(std::is_same<TInt128, TValue>::value || std::is_signed<TValue>::value, "Expected signed value");
    if (IsNormal(val)) {
        return (TValue)val;
    } else if (val == Inf()) {
        return YtDecimalInf<TValue>();
    } else if (val == -Inf()) {
        return -YtDecimalInf<TValue>();
    }
    return YtDecimalNan<TValue>();
}

inline TInt128 FromHalfs(ui64 lo, i64 hi) {
    ui64 half[2] = {lo, static_cast<ui64>(hi)};
    TInt128 val128;
    std::memcpy(&val128, half, sizeof(val128));
    return val128;
}

inline std::pair<ui64, ui64> MakePair(const TInt128 v) {
    std::pair<ui64, ui64> r;
    std::memcpy(&r, &v, sizeof(v));
    return r;
    static_assert(sizeof(r) == sizeof(v), "Bad pair size.");
}

bool IsValid(const TStringBuf& str);

// Round to nearest, ties to even.
TInt128 Div(TInt128 a, TInt128 b); // a/b
TInt128 Mul(TInt128 a, TInt128 b); // a*b
TInt128 Mod(TInt128 a, TInt128 b); // a%b

// a*b/c Only for non zero even normal positive divider.
TInt128 MulAndDivNormalDivider(TInt128 a, TInt128 b, TInt128 c);
// a*b/c Only for non zero normal positive multiplier.
TInt128 MulAndDivNormalMultiplier(TInt128 a, TInt128 b, TInt128 c);

struct TDecimal {
    TInt128 Value = 0;

    TDecimal() = default;

    template<typename T>
    TDecimal(T t): Value(t) { }

    explicit operator TInt128() const {
        return Value;
    }

    TDecimal& operator+=(TDecimal right) {
        const auto l = Value;
        const auto r = right.Value;
        const auto a = l + r;
        if (IsNormal(l) && IsNormal(r) && IsNormal(a)) {
            Value = a;
        } else if (IsNan(l) || IsNan(r)) {
            Value = Nan();
        } else {
            Value = a > 0
                ? +Inf()
                : -Inf();
        }
        return *this;
    }

    TDecimal& operator*=(TDecimal right) {
        Value = Mul(Value, right.Value);
        return *this;
    }

    TDecimal& operator/=(TDecimal right) {
        Value = Div(Value, right.Value);
        return *this;
    }

    friend TDecimal operator+(TDecimal left, TDecimal right) {
        left += right;
        return left;
    }

    friend TDecimal operator*(TDecimal left, TDecimal right) {
        left *= right;
        return left;
    }

    friend TDecimal operator/(TDecimal left, TDecimal right) {
        left /= right;
        return left;
    }
};

}
}
