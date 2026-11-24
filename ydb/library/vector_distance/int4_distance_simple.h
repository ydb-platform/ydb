#pragma once

#include "int4.h"
#include <library/cpp/dot_product/common.h>

namespace NVectorDistance::NInt4::NSimple {
    float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept;
    float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept;
    float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept;
    TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept;
    float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept;
    float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept;
    float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept;
    TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept;
}
