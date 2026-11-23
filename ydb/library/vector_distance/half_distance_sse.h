#pragma once

#include "float16.h"

#include <library/cpp/dot_product/common.h>

#include <util/system/compiler.h>
#include <util/system/types.h>

Y_PURE_FUNCTION float L1DistanceSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float L2SqrDistanceSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float DotProductSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProductSse(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;

Y_PURE_FUNCTION float L1DistanceSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float L2SqrDistanceSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float DotProductSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProductSse(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
