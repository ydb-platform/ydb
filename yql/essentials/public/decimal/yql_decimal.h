#pragma once

#include <util/generic/strbuf.h>
#include "yql_wide_int.h"

#include <type_traits>
#include <limits>

namespace NYql::NDecimal {

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

template <ui8 Scale>
struct TDivider;
#if defined(__clang__) && defined(DONT_USE_NATIVE_INT128)
template <>
struct TDivider<0> {
    static inline constexpr TUint128 Value = 1U;
};
template <ui8 Scale>
struct TDivider {
    static inline constexpr TInt128 Value = TDivider<Scale - 1U>::Value * 10U;
};
#else
template <>
struct TDivider<0> {
    static constexpr TUint128 Value = 1U;
};
template <ui8 Scale>
struct TDivider {
    static constexpr TUint128 Value = TDivider<Scale - 1U>::Value * 10U;
};
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

template <ui8 Precision>
inline constexpr TUint128 GetDivider() {
    return TDivider<Precision>::Value;
}

template <ui8 Precision, bool IncLow = false, bool DecHigh = false>
inline constexpr std::pair<TInt128, TInt128> GetBounds() {
    return std::make_pair(-GetDivider<Precision>() + (IncLow ? 1 : 0), +GetDivider<Precision>() - (DecHigh ? 1 : 0));
}

bool IsError(TInt128 v);
bool IsNan(TInt128 v);
bool IsInf(TInt128 v);

bool IsNormal(TInt128 v);
bool IsComparable(TInt128 v);

template <ui8 Precision>
inline bool IsNormal(TInt128 v) {
    const auto& b = GetBounds<Precision>();
    return v > b.first && v < b.second;
}

const char* ToString(TInt128 v, ui8 precision, ui8 scale = 0);
TInt128 FromString(const TStringBuf& str, ui8 precision, ui8 scale = 0);

// Accept string representation with exponent.
TInt128 FromStringEx(const TStringBuf& str, ui8 precision, ui8 scale);

template <typename TMkqlProto>
inline TInt128 FromProto(const TMkqlProto& val) {
    std::array<ui64, 2> half = {val.GetLow128(), val.GetHi128()};
    TInt128 val128;
    std::memcpy(&val128, half.data(), sizeof(val128));
    return val128;
}

template <typename TValue>
inline constexpr TValue YtDecimalNan() {
    return std::numeric_limits<TValue>::max();
}

template <>
inline constexpr TInt128 YtDecimalNan<TInt128>() {
    return ~(TInt128(1) << 127);
}

template <typename TValue>
inline constexpr TValue YtDecimalInf() {
    return YtDecimalNan<TValue>() - 1;
}

template <typename TValue>
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

template <typename TValue>
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
    std::array<ui64, 2> half = {lo, static_cast<ui64>(hi)};
    TInt128 val128;
    std::memcpy(&val128, half.data(), sizeof(val128));
    return val128;
}

inline std::pair<ui64, ui64> MakePair(const TInt128 v) {
    struct TPair {
        ui64 FirstHalf;
        ui64 SecondHalf;
    } r = std::bit_cast<TPair>(v);
    return std::make_pair(r.FirstHalf, r.SecondHalf);
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

    template <typename T>
    TDecimal(T t) // NOLINT(google-explicit-constructor)
        : Value(t)
    {
    }

    explicit operator TInt128() const {
        return Value;
    }

    TDecimal& operator+=(TDecimal right) {
        const auto l = Value;
        const auto r = right.Value;
        const auto a = l + r;
        if (IsNormal(l) && IsNormal(r) && IsNormal(a)) {
            Value = a;
        } else if (IsNan(l) || IsNan(r) || !a /* inf - inf*/) {
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

template <typename TRight>
class TDecimalMultiplicator {
protected:
    const TInt128 Bound_;

public:
    explicit TDecimalMultiplicator(
        ui8 precision,
        ui8 scale = 0 /* unused */)
        : Bound_(GetDivider(precision))
    {
        Y_UNUSED(scale);
    }

    TInt128 Do(TInt128 left, TRight right) const {
        TInt128 mul = Mul(left, right);

        if (mul > -Bound_ && mul < +Bound_) {
            return mul;
        }

        return IsNan(mul) ? Nan() : (mul > 0 ? +Inf() : -Inf());
    }
};

template <>
class TDecimalMultiplicator<TInt128> {
protected:
    const TInt128 Bound_;
    const TInt128 Divider_;

public:
    TDecimalMultiplicator(
        ui8 precision,
        ui8 scale)
        : Bound_(GetDivider(precision))
        , Divider_(GetDivider(scale))
    {
    }

    TInt128 Do(TInt128 left, TInt128 right) const {
        TInt128 mul = Divider_ > 1 ? MulAndDivNormalDivider(left, right, Divider_) : Mul(left, right);

        if (mul > -Bound_ && mul < +Bound_) {
            return mul;
        }

        return IsNan(mul) ? Nan() : (mul > 0 ? +Inf() : -Inf());
    }
};

template <typename TRight>
class TDecimalDivisor {
public:
    explicit TDecimalDivisor(
        ui8 precision = 0 /* unused */,
        ui8 scale = 0 /* unused */)
    {
        Y_UNUSED(precision);
        Y_UNUSED(scale);
    }

    TInt128 Do(TInt128 left, TRight right) const {
        return Div(left, right);
    }
};

template <>
class TDecimalDivisor<TInt128> {
protected:
    const TInt128 Bound_;
    const TInt128 Divider_;

public:
    TDecimalDivisor(
        ui8 precision,
        ui8 scale)
        : Bound_(GetDivider(precision))
        , Divider_(GetDivider(scale))
    {
    }

    TInt128 Do(TInt128 left, TInt128 right) const {
        TInt128 div = MulAndDivNormalMultiplier(left, Divider_, right);
        if (div > -Bound_ && div < +Bound_) {
            return div;
        }

        return IsNan(div) ? Nan() : (div > 0 ? +Inf() : -Inf());
    }
};

template <typename TRight>
class TDecimalRemainder {
protected:
    const TInt128 Bound_;
    const TInt128 Divider_;

public:
    TDecimalRemainder(
        ui8 precision,
        ui8 scale)
        : Bound_(NYql::NDecimal::GetDivider(precision - scale))
        , Divider_(NYql::NDecimal::GetDivider(scale))
    {
    }

    TInt128 Do(TInt128 left, TRight right) const {
        if constexpr (std::is_signed<TRight>::value) {
            if (TInt128(right) >= +Bound_ || TInt128(right) <= -Bound_) {
                return left;
            }
        } else {
            if (TInt128(right) >= Bound_) {
                return left;
            }
        }

        return Mod(left, Mul(Divider_, right));
    }
};

template <>
class TDecimalRemainder<TInt128> {
public:
    explicit TDecimalRemainder(
        ui8 precision = 0 /*unused*/,
        ui8 scale = 0 /*unused*/)
    {
        Y_UNUSED(precision);
        Y_UNUSED(scale);
    }

    TInt128 Do(TInt128 left, TInt128 right) const {
        return NYql::NDecimal::Mod(left, right);
    }
};

} // namespace NYql::NDecimal
