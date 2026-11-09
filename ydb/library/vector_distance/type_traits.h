#pragma once

#include "float16.h"
#include "float8.h"

#include <type_traits>

namespace NVectorDistance {

    template <typename T>
    inline constexpr bool IsReducedPrecisionFloat = std::is_same_v<T, TFloat16>
        || std::is_same_v<T, TBFloat16>
        || std::is_same_v<T, TFloat8E4M3>
        || std::is_same_v<T, TFloat8E5M2>;

    template <typename T>
    using TArithmeticCoord = std::conditional_t<IsReducedPrecisionFloat<T>, float, T>;

} // namespace NVectorDistance
