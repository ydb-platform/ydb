#pragma once

#include "float16.h"
#include "int4.h"

#include <type_traits>

namespace NVectorDistance {

    template <typename T>
    inline constexpr bool IsReducedPrecisionFloat = std::is_same_v<T, TFloat16>
        || std::is_same_v<T, TBFloat16>;

    template <typename T>
    using TArithmeticCoord = std::conditional_t<IsReducedPrecisionFloat<T>, float, std::conditional_t<IsInt4<T>, int, T>>;

} // namespace NVectorDistance
