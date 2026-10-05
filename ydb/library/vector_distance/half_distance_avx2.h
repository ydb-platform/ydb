#pragma once

#include "float16.h"

#include <library/cpp/dot_product/common.h>

#include <util/system/compiler.h>
#include <util/system/types.h>

Y_PURE_FUNCTION float L1DistanceAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float L2SqrDistanceAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float DotProductAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProductAvx2(const TFloat16* lhs, const TFloat16* rhs, size_t length) noexcept;

Y_PURE_FUNCTION float L1DistanceAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float L2SqrDistanceAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION float DotProductAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
Y_PURE_FUNCTION TTriWayDotProduct<float> TriWayDotProductAvx2(const TBFloat16* lhs, const TBFloat16* rhs, size_t length) noexcept;
