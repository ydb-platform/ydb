#pragma once 
 
#include <util/system/types.h>
#include <cmath> 
#include <optional>
 
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

}
