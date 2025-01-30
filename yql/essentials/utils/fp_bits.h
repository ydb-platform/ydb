#pragma once
#include <util/system/defaults.h>

#include <cmath>
#include <limits>

namespace NYql {
/*

double

visual c++

fff8000000000000 0.0/0.0
7ff8000000000001 snan
7ff8000000000000 qnan

gcc/clang

7ff8000000000000 0.0/0.0
7ff4000000000000 snan
7ff8000000000000 qnan

float

visual c++

ffc00000 0.0f/0.0f
7fc00001 snan
7fc00000 qnan

gcc/clang

7fc00000 0.0f/0.0f
7fa00000 snan
7fc00000 qnan

*/

template <typename T>
struct TFpTraits {
    static constexpr bool Supported = false;
};

template <>
struct TFpTraits<float> {
    static constexpr bool Supported = std::numeric_limits<float>::is_iec559;
    using TIntegral = ui32;
    static constexpr TIntegral SignMask = (1u << 31);
    static constexpr TIntegral Mantissa = 23;
    static constexpr TIntegral Exp = 8;
    static constexpr TIntegral MaxMantissa = (1u << Mantissa) - 1;
    static constexpr TIntegral MaxExp = (1u << Exp) - 1;
    static constexpr TIntegral QNan = 0x7fc00000u;
};

template <>
struct TFpTraits<double> {
    static constexpr bool Supported = std::numeric_limits<double>::is_iec559;
    using TIntegral = ui64;
    static constexpr TIntegral SignMask = (1ull << 63);
    static constexpr TIntegral Mantissa = 52;
    static constexpr TIntegral Exp = 11;
    static constexpr TIntegral MaxMantissa = (1ull << Mantissa) - 1;
    static constexpr TIntegral MaxExp = (1ull << Exp) - 1;
    static constexpr TIntegral QNan = 0x7ff8000000000000ull;
};

template <typename T, bool>
struct TCanonizeFpBitsImpl {
};

template <typename T>
struct TCanonizeFpBitsImpl<T, true> {
    static void Do(void* buffer) {
        using TTraits = TFpTraits<T>;
        using TIntegral = typename TTraits::TIntegral;
        const TIntegral value = *(TIntegral*)(buffer);
        if (value == TTraits::SignMask) {
            *(TIntegral*)buffer = 0;
            return;
        }

        const TIntegral exp = (value >> TTraits::Mantissa) & TTraits::MaxExp;
        // inf or nan
        if (exp == TTraits::MaxExp) {
            if (value & TTraits::MaxMantissa) {
                // quiet nan
                *(TIntegral*)buffer = TTraits::QNan;
            }
        }
    }
};

template <typename T>
struct TCanonizeFpBitsImpl<T, false> {
    static void Do(void* buffer) {
        using TNumTraits = std::numeric_limits<T>;
        const T value = *(T*)buffer;
        switch (std::fpclassify(value)) {
        case FP_NAN:
            static_assert(TNumTraits::has_quiet_NaN, "no QNAN");
            *(T*)buffer = TNumTraits::quiet_NaN();
            break;
        case FP_ZERO:
            *(T*)buffer = T(0);
            break;
        }
    }
};

/*
    Canonize floating point number bits.
    Converts minus zero to zero and all NaNs to quiet NaN.
    @param buffer[in/out] - aligned buffer with floating point number
*/
template <typename T>
void CanonizeFpBits(void* buffer) {
    return TCanonizeFpBitsImpl<T, TFpTraits<T>::Supported>::Do(buffer);
}

}
