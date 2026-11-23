#pragma once

#include "float8.h"

#include <library/cpp/dot_product/common.h>
#include <util/system/compiler.h>
#include <util/system/types.h>

namespace NVectorDistance {
    Y_PURE_FUNCTION float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept;

    Y_PURE_FUNCTION float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float L2Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept;
    Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept;
}
