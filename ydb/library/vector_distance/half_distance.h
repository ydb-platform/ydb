#pragma once

#include "float16.h"

#include <library/cpp/dot_product/common.h>

#include <util/system/compiler.h>
#include <util/system/types.h>

namespace NVectorDistance {
    Y_PURE_FUNCTION float L1Distance(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2SqrDistance(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2Distance(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float DotProduct(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProduct(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;

    Y_PURE_FUNCTION float L1Distance(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2SqrDistance(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2Distance(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float DotProduct(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProduct(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
}
