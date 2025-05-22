#pragma once

#include <util/system/types.h>
#include <cmath>
#include <optional>
#include <fenv.h>

namespace NMathUdf {

template <class T>
inline T RoundToDecimal(T v, int decShift) {
    T div = std::pow(T(10), decShift);
    return std::floor(v / div + T(0.5)) * div;
}

inline std::optional<i64> Mod(i64 value, i64 m) {
    if (!m) {
        return {};
    }

    const i64 result = value % m;
    if ((result < 0 && m > 0) || (result > 0 && m < 0)) {
        return result + m;
    }
    return result;
}

inline std::optional<i64> Rem(i64 value, i64 m) {
    if (!m) {
        return {};
    }

    const i64 result = value % m;
    if (result < 0 && value > 0) {
        return result + m;
    }

    if (result > 0 && value < 0) {
        return result - m;
    }
    return result;
}

inline std::optional<i64> NearbyIntImpl(double value, decltype(FE_DOWNWARD) mode) {
    if (!::isfinite(value)) {
        return {};
    }

    auto prevMode = ::fegetround();
    ::fesetround(mode);
    auto res = ::nearbyint(value);
    ::fesetround(prevMode);
    // cast to i64 gives wrong sign above 9223372036854774784
    // lower bound is adjusted to -9223372036854774784 as well
    if (res < double(std::numeric_limits<i64>::min() + 513) || res > double(std::numeric_limits<i64>::max() - 512)) {
        return {};
    }
   
    return static_cast<i64>(res);
}

inline std::optional<i64> NearbyInt(double value, ui32 mode) {
    switch (mode) {
    case 0:
        return NearbyIntImpl(value, FE_DOWNWARD);
    case 1:
        return NearbyIntImpl(value, FE_TONEAREST);
    case 2:
        return NearbyIntImpl(value, FE_TOWARDZERO);
    case 3:
        return NearbyIntImpl(value, FE_UPWARD);
    default:
        return {};
    }
}

}
